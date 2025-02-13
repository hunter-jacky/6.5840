import os, time

for i in range(1000):
    os.system("echo ''")
    os.system("echo ''")
    os.system("echo ''")
    print(i)
    os.system(f"echo 'TestManyElections3A  {i}' >> tmp")
    os.system("go test -run TestManyElections3A -race >> tmp")
    time.sleep(1)
