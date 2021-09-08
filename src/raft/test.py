import os
from multiprocessing import Pool

def run(i):
    ret = os.system("go test --run TestBackup2B > %d.log" %i)
    print("%d done ret = %d" %(i, ret))
    if ret == 0:
        os.systen("rm -f %d.log" %i)

if __name__ == "__main__":
    pool = Pool(5)
    for i in range(10):
        pool.apply_async(run, args=(i, ))
    pool.close()
    pool.join()
    print("wait for task done")
    print("done")

# "go test -run TestPersist22C" 
# 卡在选取阶段了，因为1时间最短，2，3日志长