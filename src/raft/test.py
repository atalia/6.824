import os
from multiprocessing import Pool

def run(i):
    ret = os.system("go test --run TestFigure82C > %d.log" %i)
    print("%d done ret = %d" %(i, ret))
    if ret == 0:
        os.system("rm -f %d.log" %i)

if __name__ == "__main__":
    pool = Pool(10)
    for i in range(100):
        pool.apply_async(run, args=(i, ))
    pool.close()
    pool.join()
    print("wait for task done")
    print("done")

# "go test -run TestPersist22C" 
# 卡在选取阶段了，因为1时间最短，2，3日志长：优化方法：如果follower拒绝vote，那么不更新心跳时间

# "go test --run TestFigure82C" commit 需要注意只count当前term的，就的log不需要保证


