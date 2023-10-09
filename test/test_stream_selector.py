import binanceapi.stream_selection as sel

def test_selection_init(exchange='binance',
                        datatype='trade', symboltype='option'):
    obj = sel.StreamSelection(exchange, datatype, symboltype)
    print(obj.symbols)
    print(obj.stream_list)

