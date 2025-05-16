test:
	go clean -testcache && go test -v -failfast ./...

postgres:
	docker run --rm --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=password postgres
