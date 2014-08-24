using System;
using System.Collections.Generic; 
using System.Diagnostics;
#if !SILVERLIGHT
using System.Collections.Concurrent;
#endif
using System.Threading.Tasks;
using System.Threading; 


namespace Couchbase.Lite.Util
{
    sealed class SingleThreadTaskScheduler : TaskScheduler 
    { 
        [ThreadStatic] 
        private static bool allowInlining; 
#if SILVERLIGHT
        private readonly Queue<Task> queue = new Queue<Task>();
#else
        private readonly BlockingCollection<Task> queue = new BlockingCollection<Task>(new ConcurrentQueue<Task>());
#endif
        private const int maxConcurrency = 1;
        private int runningTasks = 0;

        /// <summary>Queues a task to the scheduler.</summary> 
        /// <param name="task">The task to be queued.</param> 
        protected override void QueueTask(Task task) 
        { 
#if SILVERLIGHT
            queue.Enqueue(task);
#else
            queue.Add (task); 
#endif
            if (runningTasks < maxConcurrency)
            {
                ++runningTasks; 
                QueueThreadPoolWorkItem (); 
            }
        } 

        private void QueueThreadPoolWorkItem() 
        { 
#if SILVERLIGHT
            ThreadPool.QueueUserWorkItem(s =>
#elif STORE
            var asyncAction = Windows.System.Threading.ThreadPool.RunAsync(s =>
#else
            ThreadPool.UnsafeQueueUserWorkItem(s => 
#endif
                {
                    allowInlining = true;
                    try
                    {
                        while (true)
                        {
                            Task task;
                            if (queue.Count == 0)
                            {
                                --runningTasks;
                                break;
                            }

#if SILVERLIGHT
                            task = queue.Dequeue();
#else
                            task = queue.Take();
#endif
                            var success = TryExecuteTask(task);
                            if (!success && task.Status != TaskStatus.Canceled && task.Status != TaskStatus.RanToCompletion)
                                // TODO: Refactor to use Log.D instead.
#if SILVERLIGHT || STORE
                                Debug.WriteLine("Scheduled task failed to execute.\r\n{0}", task.Exception.ToString());
#else
                                Trace.TraceError("Scheduled task failed to execute.", task.Exception.ToString());
#endif
                        }
                    }
                    catch (Exception e)
                    {
                        // TODO: Refactor to use Log.D instead.
#if SILVERLIGHT || STORE
                        Debug.WriteLine("Scheduled task failed to execute.\r\n{0}", e.ToString());
#else
                        Trace.TraceError("Unhandled exception in runloop", e.ToString());
#endif
                        throw;
                    }
                    finally
                    {
                        allowInlining = false;
                    }
#if !STORE
                    }, null);
#else
                }, Windows.System.Threading.WorkItemPriority.Normal);
            asyncAction.GetResults();
#endif
        } 

        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) 
        { 
//            if (!allowInlining)
//                return false; 

            if (taskWasPreviouslyQueued)
                TryDequeue(task); 

            return TryExecuteTask(task); 
        } 

        protected override bool TryDequeue(Task task) 
        {
            // Our concurrent collection does not let
            // use efficiently re-order the queue,
            // so we won't try to.
            return false;
        } 

        public override int MaximumConcurrencyLevel { 
            get { 
                return maxConcurrency; 
            } 
        } 
        protected override IEnumerable<Task> GetScheduledTasks() 
        { 
            return queue.ToArray(); 
        } 
    } 
}