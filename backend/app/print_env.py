import asyncio
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.db import session_scope
from app.models import Env, DomainGrouping, MatchCondition, Destination

async def main():
    async with session_scope() as session:
        stmt = (
            select(Env)
            .options(
                selectinload(Env.source),
                selectinload(Env.domain_groupings)
                    .selectinload(DomainGrouping.match_conditions)
                    .selectinload(MatchCondition.values),
                selectinload(Env.domain_groupings)
                    .selectinload(DomainGrouping.destinations)
            )
        )
        envs = (await session.execute(stmt)).scalars().all()
        if not envs:
            print("NO ENVS FOUND")
        for env in envs:
            print("ENV:", env.id, env.name)
            for dg in env.domain_groupings:
                print("  DG:", dg.name)
                for mc in dg.match_conditions:
                    print("    MC key_path:", mc.key_path, "operator:", mc.operator, "values:", [v.value for v in mc.values])
                for dest in dg.destinations:
                    print("    DEST topic:", dest.topic)

if __name__ == "__main__":
    asyncio.run(main())
