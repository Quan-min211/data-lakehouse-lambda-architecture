# hadoop/bin/

Thu muc nay danh cho winutils.exe (Hadoop Windows binary).
Can thiet khi chay Spark Structured Streaming truc tiep tren Windows Host.

## Cach cai dat winutils.exe

1. Tai winutils.exe phu hop voi Hadoop 3.x:
   https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.5/bin

2. Copy winutils.exe vao thu muc nay:
   hadoop/bin/winutils.exe

3. Chay lai Speed Layer:
   python src/speed_layer/spark_streaming.py --once

## Khuyen nghi: Chay qua Docker (khong can winutils.exe)

   docker compose up -d speed-layer
   docker compose logs -f speed-layer

## Giai thich

Spark Structured Streaming tren Windows yeu cau Hadoop native binaries
de xu ly filesystem operations (vi du: ghi checkpoint, doc/ghi file tam).

Khi chay trong Docker container Linux, van de nay khong ton tai vi
container da co sac san Hadoop environment dung cach.
