"""Apply a reviewed, exact-row repair without classification or filing side effects."""
from __future__ import annotations

from sqlalchemy import select, and_
from packages.db import Base

ALLOWED_TABLES = {'raw_messages', 'message_decisions', 'incidents', 'incident_witnesses'}


def apply_audited_patch(session, patch: dict, *, dry_run: bool = False) -> dict:
    """Caller owns commit/rollback and publication approval. Fail on any drift."""
    if patch.get('schema_version') != 1 or not isinstance(patch.get('changes'), list):
        raise ValueError('Invalid audited repair manifest')
    operations = []
    seen = set()
    for change in patch['changes']:
        name = change['table']
        if name not in ALLOWED_TABLES:
            raise ValueError('Repair cannot mutate filing or other protected tables')
        table = Base.metadata.tables[name]
        keys = {c.name for c in table.primary_key}
        if set(change['key']) != keys:
            raise ValueError('Repair requires exact primary key')
        identity = (name, tuple(sorted(change['key'].items())))
        if identity in seen:
            raise ValueError('Repeated repair row')
        seen.add(identity)
        condition = and_(*(table.c[k] == v for k, v in change['key'].items()))
        before, after = change['before'], change['after']
        for row in (before, after):
            if row is not None and (set(row) != {c.name for c in table.columns} or any(row[k] != v for k,v in change['key'].items())):
                raise ValueError('Repair row schema or identity changed')
        current = session.execute(select(table).where(condition).with_for_update()).mappings().first()
        current = dict(current) if current is not None else None
        if current == after:
            continue
        if current != before:
            raise ValueError(f'Audited repair is stale for {name}; regenerate preview')
        # Allow the existing exact-alias metadata promotion, but no new or
        # semantically changed raw evidence. Original rows remain in the manifest.
        if name == 'raw_messages' and after is not None:
            from packages.tasker_capture import cross_source_text_signature
            if (before is None or cross_source_text_signature(before['text']) != cross_source_text_signature(after['text'])
                    or abs(int(before['ts_epoch'] or 0)-int(after['ts_epoch'] or 0)) > 120):
                raise ValueError('Repair cannot rewrite raw evidence')
        if name == 'incident_witnesses' and before is None:
            raise ValueError('Historical repair must reuse allocated witness IDs; never outrun the database sequence')
        operations.append((table, condition, before, after))
    if dry_run:
        return {'would_change_rows': len(operations), 'filing_jobs_created': 0, 'dry_run': True}
    # All preconditions have passed before the first write. Respect foreign keys.
    order = {table.name: index for index,table in enumerate(Base.metadata.sorted_tables)}
    # Witnesses are leaf rows with a composite unique key. Replace the exact
    # reviewed set first so an ID reassignment cannot collide with its old row.
    witnesses = [op for op in operations if op[0].name == 'incident_witnesses']
    for table, condition, before, after in witnesses:
        if before is not None:
            session.execute(table.delete().where(condition))
    ordinary = [op for op in operations if op[0].name != 'incident_witnesses']
    for table, condition, before, after in sorted(ordinary, key=lambda x:order[x[0].name]):
        if before is None and after is not None:
            session.execute(table.insert().values(**after))
    for table, condition, before, after in ordinary:
        if before is not None and after is not None:
            session.execute(table.update().where(condition).values(**after))
    for table, condition, before, after in sorted(ordinary, key=lambda x:order[x[0].name], reverse=True):
        if after is None:
            session.execute(table.delete().where(condition))
    for table, condition, before, after in witnesses:
        if after is not None:
            session.execute(table.insert().values(**after))
    session.expire_all()
    return {'changed_rows': len(operations), 'filing_jobs_created': 0}
