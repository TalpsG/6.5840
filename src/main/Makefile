worker:
	go build -buildmode=plugin ../mrapps/wc.go
	go run ./mrworker.go wc.so > output1 & 
	go run ./mrworker.go wc.so > output2 & 
	go run ./mrworker.go wc.so > output3 &
	go run ./mrworker.go wc.so > output4 &

co:
	go run ./mrcoordinator.go wc.so pg-*.txt
