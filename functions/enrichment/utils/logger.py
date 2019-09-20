from typing import Generator

def log_generator(iterations: int, xs: Generator[str, None, None]) -> ():
    counter = 0
    for x in xs:
        if counter == iterations:
            break
        else:
            counter += 1
            print(x)
            print(len(x))
