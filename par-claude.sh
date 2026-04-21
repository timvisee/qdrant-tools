#!/usr/bin/env bash

set -euo pipefail

CREATE_BRANCH=0

usage() {
    echo "Usage: $(basename "$0") [-c] <branch-name>"
    echo ""
    echo "  -c  Create a new branch"
    exit 1
}

while getopts "c" opt; do
    case $opt in
        c) CREATE_BRANCH=1 ;;
        *) usage ;;
    esac
done
shift $((OPTIND - 1))

if [[ $# -lt 1 ]]; then
    usage
fi

BRANCH=$1
WORKTREE_DIR="worktree-$BRANCH"
ORIGINAL_DIR="$(pwd)"

if [[ $CREATE_BRANCH -eq 1 ]]; then
    git worktree add -b "$BRANCH" "$WORKTREE_DIR"
else
    git worktree add "$WORKTREE_DIR" "$BRANCH"
fi

echo "Created worktree '$WORKTREE_DIR' for branch '$BRANCH'"

if [[ -d "$ORIGINAL_DIR/target" ]]; then
    ln -s "$ORIGINAL_DIR/target" "$ORIGINAL_DIR/$WORKTREE_DIR/target"
fi

cd "$ORIGINAL_DIR/$WORKTREE_DIR"
claude || true

cd "$ORIGINAL_DIR"
read -r -p "Clean up worktree $WORKTREE_DIR? [y/N] " REPLY
if [[ "$REPLY" =~ ^[Yy]$ ]]; then
    git worktree remove "$WORKTREE_DIR"
    echo "Worktree '$WORKTREE_DIR' removed, branch '$BRANCH' still exists"
else
    echo "Worktree kept at $WORKTREE_DIR"
fi
