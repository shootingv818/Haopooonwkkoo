"""
Unified entrypoint — one codebase, three roles (config.MODE):

    MODE=owner     -> central panel bot          (owner_bot.py)
    MODE=customer  -> customer subscription bot   (customer_bot.py)
    MODE=worker    -> headless Rubika worker node (worker_api.py)

The two bots are SEPARATE processes with SEPARATE tokens and SEPARATE databases;
run two instances (one with MODE=owner, one with MODE=customer). Workers are
provisioned automatically by the owner panel and run MODE=worker.

Run locally:
    MODE=owner    python main.py
    MODE=customer python main.py
"""
import config


def main():
    mode = config.MODE
    if mode == "worker":
        import worker_api
        worker_api.run()
    elif mode == "customer":
        import asyncio
        import customer_bot
        asyncio.run(customer_bot.amain())
    else:  # owner (default)
        import asyncio
        import owner_bot
        asyncio.run(owner_bot.amain())


if __name__ == "__main__":
    main()
