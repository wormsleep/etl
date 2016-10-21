package zw.wormsleep.tools.etl.multitask.multithread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zw.wormsleep.tools.etl.multitask.MultiTaskMultiThread;
import zw.wormsleep.tools.etl.multitask.Task;

import java.util.ArrayList;
import java.util.List;

/**
 * N个任务,平均分给M线程处理
 *
 * @author zhaowei
 */
public class NTaskPerThread implements MultiTaskMultiThread {
    final Logger logger = LoggerFactory.getLogger(NTaskPerThread.class);

    int taskCount;
    int threadCount;
    List<Task> tasks;
    long total = 0; // 任务运行时间,用于比较不同线程数量的效率
    Task task;

    public NTaskPerThread(List<Task> tasks, int threadCount) {
        this.tasks = tasks != null ? tasks : new ArrayList<Task>();
        this.taskCount = tasks.size();
        this.threadCount = threadCount;
    }

    @Override
    public void executeBatch() {

        // 无任务直接返回
        if (taskCount < 1) return;

        // 若任务数小于线程数则令线程数等于任务数
        if (taskCount < threadCount) {
            threadCount = taskCount;
        }

        // 给每个线程分配任务,应list从索引0开始,所以分配任务编号从0开始
        int num = taskCount / threadCount;// 这样子可能还有余数,应该把余数也分摊
        if (taskCount % threadCount != 0) {
            num++;// 如果有余数(一定小于thread_num),则前面的线程分摊下,每个线程多做一个任务
        }

        logger.info("@@@ 多线程实际分配情况 - 任务总数: {} 线程总数: {} 每线程任务数: {}", taskCount, threadCount, num);

        for (int i = 0; i < threadCount; i++) {
            int start = i * num;
            int end = Math.min((i + 1) * num, tasks.size());// 最后一个线程任务可能不够
            new TaskThread(start, end).start();
        }
    }

    private class TaskThread extends Thread {
        int start;
        int end;

        public TaskThread(int start, int end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public void run() {
            long sm = System.currentTimeMillis();
            int taskCount = end - start;
            for (; start < end; start++) {
                tasks.get(start).execute();
            }
            logger.info("@@@ 线程 {} 分配任务数 {} 个共耗时 {} 毫秒.", Thread
                            .currentThread().getName(), taskCount,
                    (total + System.currentTimeMillis() - sm));
        }
    }
}
