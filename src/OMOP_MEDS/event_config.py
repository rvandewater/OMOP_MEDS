"""Helpers for the MESSY event conversion config (`configs/event_configs.yaml`) handed to MEDS-Extract."""

from omegaconf import DictConfig


def prune_event_config(
    event_cfg: DictConfig, available_tables: set[str]
) -> tuple[DictConfig, list[str]]:
    """Drop event-table blocks whose pre-MEDS table was not produced, keeping reserved MESSY keys.

    Top-level keys starting with `_` (`_defaults`, which carries the `subject_id` expression) are MESSY
    settings, not tables, and must survive: without `_defaults` every remaining table loses its subject ID.

    Args:
        event_cfg: The event conversion config, as loaded with OmegaConf.
        available_tables: Stems of the tables present in the pre-MEDS directory.

    Returns:
        The pruned copy of `event_cfg` and the names of the removed tables, in config order.

    Examples:
        >>> from omegaconf import OmegaConf
        >>> cfg = OmegaConf.create({
        ...     "_defaults": {"subject_id": "$person_id"},
        ...     "person": {"race": {"code": "RACE", "time": None}},
        ...     "note": {"note": {"code": "NOTE", "time": "$preferred_time"}},
        ... })
        >>> pruned, removed = prune_event_config(cfg, {"person", "codes"})
        >>> list(pruned.keys()), removed
        (['_defaults', 'person'], ['note'])

        The input config is not modified:

        >>> list(cfg.keys())
        ['_defaults', 'person', 'note']
    """
    removed = [
        key
        for key in event_cfg.keys()
        if not str(key).startswith("_") and key not in available_tables
    ]
    pruned = event_cfg.copy()
    for key in removed:
        pruned.pop(key)
    return pruned, removed
