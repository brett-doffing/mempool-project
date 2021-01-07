import bitcoin.rpc
import pandas as pd
import numpy as np
import sched, time

s = sched.scheduler(time.time, time.sleep)
current_block_height = 0
mempool_hashes = pd.Series()
mempool_df = pd.DataFrame(columns=['txid', 'fee', 'weight', 'bip125', 'time', 'height', 'confirmed'])

def get_data(sc): 
    global current_block_height
    global rpc
    global mempool_hashes
    global mempool_df

    bitcoin.SelectParams('mainnet')
    rpc = bitcoin.rpc.RawProxy()

    besthash = rpc.getbestblockhash()
    block = rpc.getblock(besthash)

    height = block['height']

    # If the height > current_block_height, a new block has been found
    if height > current_block_height:
        print('Block Found!!')

        # Change current_block_height
        current_block_height = height
        
        previous_mempool_hashes = mempool_hashes.copy()

        # For tx in block, see if in mempool, and flag as in block
        mempool_hashes = pd.Series(rpc.getrawmempool())
        # NOTE: Explain this
        possible_block_txs = previous_mempool_hashes[~previous_mempool_hashes.isin(mempool_hashes)]

        print('mempool_hashes({0}) - previous_mempool_hashes({1}) = possible_block_txs({2})'.format(len(mempool_hashes), 
                                                                                            len(previous_mempool_hashes), 
                                                                                            len(possible_block_txs)))

        best_block_txs = pd.Series(block['tx'])
        confirmed_txs = possible_block_txs[possible_block_txs.isin(best_block_txs)]
        print('Number of confirmed transactions:', len(confirmed_txs))

        mempool_df['confirmed'] = mempool_df.txid.isin(confirmed_txs)

        # Create csv
        # mempool_df.to_csv('pool_data/mem_blk_{0}.csv'.format(height))
        # print(mempool_df.tail())
        
    mempool_hashes = pd.Series(rpc.getrawmempool())
    mempool_df = pd.DataFrame(columns=['txid', 'fee', 'weight', 'bip125', 'time', 'height', 'confirmed'])

    for tx in mempool_hashes:
            # fees : {base,modified,ancestor,descendant}
            # vsize, weight, fee, modifiedfee, time, height, 
            # descendantcount, descendantsize, descendantfees, 
            # anscestorcount, ancestorsize, ancestorfees,
            # wtxid: '', depends: [], spentby: [], bip125-replaceable: bool
            try:
                tx_info = rpc.getmempoolentry(tx)
            except:
                continue
            

            row = {'txid' : tx, 
                    'fee' : tx_info['fee'],
                    'weight' : tx_info['weight'], 
                    'bip125' : tx_info['bip125-replaceable'], 
                    'time' : tx_info['time'], 
                    'height' : tx_info['height'],
                    'confirmed' : 0}

            mempool_df.loc[len(mempool_df)] = row

    # Call again after 30 seconds
    s.enter(30, 1, get_data, (sc,))

# Call after 30 seconds
s.enter(30, 1, get_data, (s,))
s.run()
