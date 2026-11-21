# How to run the program

## Run
Start all three nodes before any client, each in its own terminal:

```
go run ./Node -id 1
go run ./Node -id 2
go run ./Node -id 3
```

Then start one client per bidder in their own terminals:

```
go run ./client -name alice
go run ./client -name bob -node 3
```

`-node` picks which node (1–3) the client talks to first.
Client commands: `bid <amount>`, `result`, `quit`.

The auction ends 100 s after the first bid (change with `-duration`, e.g. `-duration 60s` on the nodes).

## Crash test
Press Ctrl-C in node 1's terminal and keep bidding: the client fails and node 2 takes over as leader.
`go run ./Node -id 1 -crash-before-reply` makes node 1 crash right after replicating a bid, before answering.

Each node writes its log to `nodeN.log`.