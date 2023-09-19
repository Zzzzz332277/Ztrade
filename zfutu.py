
#与富途进行通信
'''
    #################################与富途api进行连接########################################
    # 实例化行情上下文对象
    quote_ctx = ft.OpenQuoteContext(host="127.0.0.1", port=11111)
    # 上下文控制
    quote_ctx.start()  # 开启异步数据接收
    quote_ctx.set_handler(ft.TickerHandlerBase())  # 设置用于异步处理数据的回调对象(可派生支持自定义)

    ret, data = quote_ctx.get_user_security("每日关注")
    if ret == RET_OK:
        print(data)
        if data.shape[0] > 0:  # 如果自选股列表不为空
            print(data['code'][0])  # 取第一条的股票代码
            print(data['code'].values.tolist())  # 转为 list
    els e:
        print('error:', data)
    quote_ctx.close()  # 结束后记得关闭当条连接，防止连接条数用尽
    #######################################################################################
    ###'''