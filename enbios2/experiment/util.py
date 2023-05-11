from bw2data.backends import Activity


def get_connections(activity: Activity):
    return {
        "biosphere": list(activity.biosphere()),
        "technosphere": list(activity.technosphere()),
        "consumers": list(activity.consumers()),
    }


def connections_size(conns: dict[str,any]):
    return {k: len(v) for k,v in conns.items()}