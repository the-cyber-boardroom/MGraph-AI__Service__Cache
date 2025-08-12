from osbot_utils.type_safe.Type_Safe import Type_Safe


# todo: see if we need this class
class Schema__Cache__Stats__Response(Type_Safe):    # Response schema for statistics
    total_entries       : int
    total_unique_hashes : int
    total_size_bytes    : int
    total_versions      : int
    storage_used_percent: float
    cache_hit_rate      : float
    avg_entry_size      : int
    oldest_entry        : str   = None
    newest_entry        : str   = None