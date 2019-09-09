import os
from datetime import datetime
import shutil

now = datetime.now()
ts = now.strftime('%d-%m-%Y-%H-%M-%S')

def main(path: str) -> ():
    for filename in os.listdir(f'./export/{path}'):
        extention = os.path.splitext(filename)[1]
        if '.csv' in extention:
            try:
                os.mkdir(f'./export/coalesce/')
            except FileExistsError:
                shutil.move(f'./export/{path}/{filename}', f'./export/coalesce/{path}-{ts}.csv')
                print(f'Move {filename} from {path} to ./export/coalesce/')

if __name__ == '__main__':
    #main('sessions')
    main('pageviews')
    main('products')
    main('events')
