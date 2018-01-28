# Memory safety

Writing correct and performant JNI code can be a bit tricky, especially because the amount of documentation on this subject is not that great. During the development of the JNI plugin, I came accross various sources of information and best practices regarding memory access, which are documented here.

## Memory references

When writing JNI code, rather than getting access to the actual object as it is inside the JVM, you get a reference to this object. This is because the JVM's GC might decide to move this object at any point in time (this is especially true for compacting GCs, such as the G1GC or the parallel compacting GCs), and the JNI reference moves with it. (See also: [Overview of JNI object references](https://www.ibm.com/support/knowledgecenter/en/SSYKE2_8.0.0/com.ibm.java.aix.80.doc/diag/understanding/jni_refs.html)).

There are three different types of memory references:

 * a local reference, which cannot be shared between different threads/invocations, and the JVM automatically cleans all up after the JNI method returns back to the JVM;
 * a global reference, which can be shared between different threads and will stay alive until DeleteGlobalReference is called;
 * a weak global reference, which can be shared between different threads, but is will be cleaned when no one else references this object anymore.
 
The JVM, by default, only allocates a small register of local references that a JNI function can use while it's being executed (default is 16). If you create a lot of new objects while executing your JNI function, please use `EnsureLocalCapacity` and set it to a higher number. 

A pattern I'm seeing a lot myself is that these local references are "hierarchical":

* you create a new array;
* you create a lot of new objects and add these to the array;
* you continue to work only with the array.
 
In this scenario, you should *not* delete the local reference for all these new, small objects: they would be cleaned up by the JVM, and your java code will segfault.

Instead, in this scenario you can call `PushLocalFrame` and `PopLocalFrame`, which are actually used by the JVM around the invocation of your JNI function as well. You create a new local frame when you create the array, and pop the local frame using the array as "result" object. The JVM now frees all local references in the local frame, and returns a local reference to the result array.

## Memory pinning

So, these refgerences sounds good and make sense! However, if native code can run concurrently with other JVM threads and VM operations (such as GC), how can I know for sure that a memory location is not moved around when I'm working with it?

For this, the JVM employs memory pinning in several ways. Which one you should use depends what you're trying to do:

* for normal primitive types (integers, floating points, etc) the JVM just copies the values;
* for strings, you can either use
  * GetStringChars or GetStringUTFChars, both of which might copy the underlying strings
  * GetStringRegion, which always copies into a buffer provided by you;
  * GetStringCritical, which never copies but imposes severe restrictions on the JVM until it is released with ReleaseStringCritical (See "Critical regions" below)
* for byte arrays `byte[]`, you can either use
  * GetByteArrayElements, which may cause a copy, mostly depending upon the GC being used. Since this may be a copy, changes to the returned `void *` may not be reflected in the original array;
  * GetByteArrayRegion, which always copies into a buffer provided by you;
  * GetPrimitiveArrayCritical, which (almost) never copies but imposes severe restrictions on the JVM until it is released with ReleaseByteArrayCritical (See "Critical regions" below)
* in addition to `byte[]`, you can also use the NIO `ByteBuffer` and the native functions the JVM provides
  * GetDirectBufferAddress, which provides direct access to the underlying byte array

Using a `ByteBuffer` in java code is slower than just using a `byte[]` because a direct ByteBuffer's memory lives entirely outside the JVM. I have [read claims](https://stackoverflow.com/a/28799276) that creating a single-byte ByteBuffer takes longer than copying 1000 individual bytes, but YMMV.

## Critical regions

As mentioned above, the JNI has something called "critical regions" that guarantees that memory will be pinned, with almost always no copies. To ensure that these conditions hold, the JVM is put into a somewhat crippled mode, where it's unable to do almost anything.

There appears to be a lot of conflicting information on what exactly is and is not possible while the JVM is in this critical mode (different JVM vendors have different implementations as well), and whether or not a copy is still returned.

I've been able to distill the information as follows:

* the only invariant between all different docs is that the JVM guarantees the memory is stable;
* using a non-compacting GC increases your chances of acquiring a direct pointer without copies.
 
The restrictions while in critical mode are:

* you cannot invoke any JVM function;
* any compacting GC is likely to be entirely shut down (G1GC, ParallelG1GC).
 
For most intents and purposes, you should treat code inside a critical region as if it has acquired a global lock on the JVM; blocking on anything is strongly discouraged, and you should leave this mode as soon as possible.

### Recommendation

We should probably benchmark the performance difference between `ByteBuffer` and `byte[]`, especially when they are backed by large arrays with different garbage collectors. 

I am personally inclined to say that using NIO `ByteBuffer` appears to be the way to go, as long as they are backed with a direct buffer managed outside the JVM. But we need more benchmark data to know for sure, since just using `byte[]` provides more control and is simpler.

## Further reading

 * https://developer.android.com/training/articles/perf-jni.html
 * https://www.ibm.com/support/knowledgecenter/en/SSYKE2_8.0.0/com.ibm.java.aix.80.doc/diag/understanding/jni_refs.html
 * https://www.ibm.com/support/knowledgecenter/SSYKE2_8.0.0/com.ibm.java.aix.80.doc/diag/understanding/jni_copypin.html
 * https://docs.oracle.com/javase/7/docs/technotes/guides/jni/spec/functions.html#GetPrimitiveArrayCritical
 * https://github.com/fommil/netlib-java/issues/58
