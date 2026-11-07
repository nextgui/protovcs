# Security

## Reporting a vulnerability

Report a vulnerability privately through GitHub: on this repository's
**Security** tab, choose **Report a vulnerability**. Please don't open a public
issue for one.

Include what an attacker can do, the conditions it needs, and a way to
reproduce it if you have one. We will acknowledge the report and keep you
informed as it is fixed.

## What protovcs protects

protovcs is built so that the server storing a store cannot read it, cannot
make a client accept forged contents, and cannot make a client accept a head
older than the newest it has seen on that branch. A way to break any of those
is in scope.

The server can see how much is stored, in how many chunks, when they change,
and which commits are whose parents. It can refuse service or delete data.
Those are known limits, not vulnerabilities. A client also accepts a head on a
different line of history from the one it last saw, because that is what a
deliberate force-move looks like.
