#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys
from collections import deque

LEADER = 3


class Peer:
    def __init__(self, label, executable, node_id, library_path):
        env = os.environ.copy()
        env["LD_LIBRARY_PATH"] = library_path
        self.label = label
        self.proc = subprocess.Popen(
            [executable, str(node_id)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=env,
        )

    def command(self, command):
        self.proc.stdin.write(command + "\n")
        self.proc.stdin.flush()
        line = self.proc.stdout.readline().strip()
        if not line.startswith("RESULT "):
            returncode = self.proc.poll()
            stderr = self.proc.stderr.read() if returncode is not None else ""
            raise RuntimeError(
                f"{self.label}: command {command!r} invalid result {line!r} "
                f"exit={returncode} stderr={stderr!r}"
            )
        parts = line.split()
        if len(parts) != 8:
            raise RuntimeError(f"{self.label}: malformed RESULT {line!r}")
        result = {
            "rc": int(parts[1]),
            "role": int(parts[2]),
            "term": int(parts[3]),
            "leader": int(parts[4]),
            "last": int(parts[5]),
            "commit": int(parts[6]),
            "frame_count": int(parts[7]),
            "frames": [],
        }
        for _ in range(result["frame_count"]):
            frame_line = self.proc.stdout.readline().strip()
            if not frame_line.startswith("FRAME "):
                raise RuntimeError(f"{self.label}: malformed frame {frame_line!r}")
            result["frames"].append(frame_line[6:])
        end = self.proc.stdout.readline().strip()
        if end != "END":
            raise RuntimeError(f"{self.label}: missing END, got {end!r}")
        if result["rc"] != 0:
            raise RuntimeError(f"{self.label}: command {command!r} rc={result['rc']}")
        print(
            f"TRACE peer={self.label} command={command.split()[0]} "
            f"role={result['role']} term={result['term']} "
            f"leader={result['leader']} last={result['last']} "
            f"commit={result['commit']} frames={result['frame_count']}"
        )
        return result

    def stop(self):
        cleanup_errors = []
        if self.proc.poll() is None:
            try:
                self.command("STOP")
            except Exception as exc:
                cleanup_errors.append(f"STOP: {exc}")
            try:
                self.proc.stdin.close()
            except Exception as exc:
                cleanup_errors.append(f"stdin close: {exc}")
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
                cleanup_errors.append("forced kill after timeout")
        stderr = self.proc.stderr.read()
        if self.proc.returncode not in (None, 0):
            cleanup_errors.append(
                f"exit={self.proc.returncode} stderr={stderr!r}"
            )
        if cleanup_errors:
            print(
                f"CLEANUP peer={self.label} " + " | ".join(cleanup_errors),
                file=sys.stderr,
            )


def exchange(source, target, frames):
    queue = deque((target, source, frame) for frame in frames)
    delivered = 0
    while queue:
        peer, reply_peer, frame = queue.popleft()
        delivered += 1
        if delivered > 128:
            raise RuntimeError("mixed-version frame exchange did not quiesce")
        result = peer.command("STEP " + frame)
        for response in result["frames"]:
            queue.append((reply_peer, peer, response))
    return delivered


def status(peer):
    return peer.command("STATUS")


def session(leader_label, leader_bin, leader_libs,
            follower_label, follower_bin, follower_libs):
    leader = Peer(leader_label, leader_bin, 1, leader_libs)
    follower = Peer(follower_label, follower_bin, 2, follower_libs)
    delivered = 0
    try:
        result = leader.command("TICK 5 7")
        delivered += exchange(leader, follower, result["frames"])

        leader_status = status(leader)
        follower_status = status(follower)
        if leader_status["role"] != LEADER or leader_status["leader"] != 1:
            raise RuntimeError(
                f"{leader_label}: expected leader after election, got {leader_status}"
            )
        if follower_status["leader"] != 1:
            raise RuntimeError(
                f"{follower_label}: expected leader=1, got {follower_status}"
            )

        result = leader.command("PROPOSE 1 6d697865642d70656572")
        delivered += exchange(leader, follower, result["frames"])

        # Propagate the committed index to the follower through a heartbeat.
        result = leader.command("TICK 1 7")
        delivered += exchange(leader, follower, result["frames"])

        leader_status = status(leader)
        follower_status = status(follower)
        if leader_status["commit"] < 1 or follower_status["commit"] < 1:
            raise RuntimeError(
                f"commit did not cross mixed peers: leader={leader_status} "
                f"follower={follower_status}"
            )
        if leader_status["last"] < 1 or follower_status["last"] < 1:
            raise RuntimeError(
                f"log did not replicate across mixed peers: leader={leader_status} "
                f"follower={follower_status}"
            )
        print(
            f"PASS session leader={leader_label} follower={follower_label} "
            f"term={leader_status['term']} commit={leader_status['commit']} "
            f"frames={delivered}"
        )
    finally:
        leader.stop()
        follower.stop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--old-node", required=True)
    parser.add_argument("--current-node", required=True)
    parser.add_argument("--old-libs", required=True)
    parser.add_argument("--current-libs", required=True)
    args = parser.parse_args()

    session(
        "v0.2.0", args.old_node, args.old_libs,
        "current", args.current_node, args.current_libs,
    )
    session(
        "current", args.current_node, args.current_libs,
        "v0.2.0", args.old_node, args.old_libs,
    )
    print("PASS: live mixed-version wire-v6 election and commit in both leader directions")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        sys.exit(1)
