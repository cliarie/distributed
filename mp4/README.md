Instructions to test:

EXECUTE THE FOLLOWING FROM /mp4

Start hydfs on each server:
go run pkg/hydfs/server/server.go

On the leader (vm1):
export WORKER_INDEX=1 && export LEADER_ADDRESS=fa24-cs425-0701.cs.illinois.edu:50051
go run pkg/hydfs/server/server.go

On each worker (replace WORKER_INDEX=?), you may have to install a go package as prompted by terminal:
export WORKER_INDEX=2 && export LEADER_ADDRESS=fa24-cs425-0701.cs.illinois.edu:50051
go run cmd/worker/main.go


Finally, to run rainstorm:
go run ./cmd/client/main.go "./scripts/filter.exe STOP" "./scripts/transform.exe" input.csv output.csv 3