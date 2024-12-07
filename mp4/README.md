Instructions to test:

EXECUTE THE FOLLOWING FROM /mp4

Start hydfs on each server:
go run pkg/hydfs/server/server.go

On the leader (vm1):
export WORKER_INDEX=1 && export LEADER_ADDRESS=fa24-cs425-0701.cs.illinois.edu:50051
go run cmd/leader/main.go

On each worker (replace WORKER_INDEX=?), you may have to install a go package as prompted by terminal:
export WORKER_INDEX=2 && export LEADER_ADDRESS=fa24-cs425-0701.cs.illinois.edu:50051
go run cmd/worker/main.go


Finally, to run rainstorm:
go run ./cmd/client/main.go "./scripts/filter.exe STOP" "./scripts/transform.exe" input.csv output.csv 3


App 1:
cat local_input.csv | scripts/filter1.exe Streetname | scripts/transform.exe
go run ./cmd/client/main.go "./scripts/filter1.exe Streetname" "./scripts/transform.exe" input.csv output.csv 3
2,Streetname - Mast Arm
8,Streetname - Sesquicentennial
9,Streetname - Sesquicentennial
28,Streetname
45,Streetname
46,Streetname
49,Streetname
50,Streetname
51,Streetname
52,Streetname
53,Streetname
54,Streetname
55,Streetname
56,Streetname
57,Streetname
58,Streetname
59,Streetname
61,Streetname
62,Streetname
63,Streetname
64,Streetname
65,Streetname
66,Streetname
67,Streetname
68,Streetname
69,Streetname
70,Streetname
71,Streetname
72,Streetname
73,Streetname
74,Streetname
75,Streetname
76,Streetname

go run ./cmd/client/main.go "./scripts/filter1.exe Streetname" "./scripts/transform.exe" 10000.csv output.csv 3


App 2:
cat local_input.csv | scripts/filter2.exe "Punched Telespar" | scripts/aggregate.exe
go run ./cmd/client/main.go "./scripts/filter2.exe \"Punched Telespar\"" "./scripts/aggregate.exe" input.csv output.csv 3
Warning,9
Streetname,31
Parking,2
Regulatory,13


cat local_10000.csv | scripts/filter2.exe "Punched Telespar" | scripts/aggregate.exe
go run ./cmd/client/main.go "./scripts/filter2.exe \"Punched Telespar\"" "./scripts/aggregate.exe" 10000.csv output.csv 3
Parking,1355
MTD,92
Warning,243
Streetname,1334
Regulatory,1732
School,198
Object Marker,81
Custom,78
Guide,41