worker:
	go build -buildmode=plugin ../mrapps/wc.go
	go run ./mrworker.go wc.so
co:
	go run ./mrcoordinator.go wc.so pg-*.txt
