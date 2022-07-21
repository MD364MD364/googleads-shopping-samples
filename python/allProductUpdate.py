from __future__ import print_function
from asyncio.windows_events import NULL
import sys

import mariadb
from pyparsing import nums
from shopping.content.products.insert_batch import BATCH_SIZE

def main(argv):

    try:
        conn4 = mariadb.connect(
            user="root",
            password="5LUxZA2CnEmZQ8dm",
            host="127.0.0.1",
            port=3306,
            database="safetymediaapp"
        )
        print('connected')
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)
    cur4 = conn4.cursor()
    cur4.execute("UPDATE product SET statusMerchantCenter='2' WHERE 1")
    conn4.commit()
    conn4.close()

# Allow the function to be called with arguments passed from the command line.
if __name__ == '__main__':
    main(sys.argv)
