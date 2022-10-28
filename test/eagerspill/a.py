from time import perf_counter
a = 0
begin = perf_counter()
for i in range(100000000):
    if i%2 == 0:
        a += 1
    else:
        a -=1
end = perf_counter()
print(end-begin)
