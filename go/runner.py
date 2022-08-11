import os
import subprocess
import time

BASE_CLIENT_PORT = 5101


def run_test(go_max_procs, iperfs):
    print(go_max_procs, iperfs, '*****')
    env = dict(os.environ)
    with subprocess.Popen(['./main'],
                          env={**env, 'GOMAXPROCS': str(go_max_procs)},
                          ) as relay:

        time.sleep(2)

        ps = []
        for iperf in range(iperfs):
            p = subprocess.Popen(['iperf3', '-c', 'localhost', '--port', str(BASE_CLIENT_PORT + iperf)])
            ps.append(p)

        for p in ps:
            p.communicate()
            p.wait()

        relay.kill()
        time.sleep(1)
        relay.wait(1)


def main():
    # run_test(1, 1)
    # return
    for i in range(1, 5):
        for j in range(1, 5):
            go_max_procs = i
            iperfs = j

            run_test(go_max_procs, iperfs)


if __name__ == '__main__':
    main()
