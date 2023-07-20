import asyncio


def chunk(complete_list: list, chunck_size: int) -> list:
    """
    Function to partition a list in chunks
    :param complete_list: List you want to partition
    :param chunck_size: Size of chunks
    :return: List with chunks (List o lists)
    """
    return [complete_list[x: x + chunck_size] for x in range(0, len(complete_list), chunck_size)]


def run_async(f):
    def wrapped(*args, **kwargs):
        return asyncio.new_event_loop().run_in_executor(None, f, *args, *kwargs)

    return wrapped
