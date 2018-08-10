set worker=2
call TransactionBenchmarkTest.exe -record=1000000 -worker_per_redis=1 -worker=%worker% -load=true -clear=true -run=false
start TransactionBenchmarkTest.exe -record=1000000 -workload=100000 -worker_per_redis=1 -worker=%worker% -pipeline=400 -type=hybrid -scale=0.9 -load=false -clear=false -run=true -dist=zipf -readperc=0.8 -query=2 -process_offset=0
start TransactionBenchmarkTest.exe -record=1000000 -workload=100000 -worker_per_redis=1 -worker=%worker% -pipeline=400 -type=hybrid -scale=0.9 -load=false -clear=false -run=true -dist=zipf -readperc=0.8 -query=2 -process_offset=1
start TransactionBenchmarkTest.exe -record=1000000 -workload=100000 -worker_per_redis=1 -worker=%worker% -pipeline=400 -type=hybrid -scale=0.9 -load=false -clear=false -run=true -dist=zipf -readperc=0.8 -query=2 -process_offset=2
start TransactionBenchmarkTest.exe -record=1000000 -workload=100000 -worker_per_redis=1 -worker=%worker% -pipeline=400 -type=hybrid -scale=0.9 -load=false -clear=false -run=true -dist=zipf -readperc=0.8 -query=2 -process_offset=3
start TransactionBenchmarkTest.exe -record=1000000 -workload=100000 -worker_per_redis=1 -worker=%worker% -pipeline=400 -type=hybrid -scale=0.9 -load=false -clear=false -run=true -dist=zipf -readperc=0.8 -query=2 -process_offset=4
start TransactionBenchmarkTest.exe -record=1000000 -workload=100000 -worker_per_redis=1 -worker=%worker% -pipeline=400 -type=hybrid -scale=0.9 -load=false -clear=false -run=true -dist=zipf -readperc=0.8 -query=2 -process_offset=5
