## eredis_drive


Erlang Redis 驱动</br>
配置
```
{env, [
            {mode, cluster},
            {pools, {
                [{worker_module,eredis_conn},{size, 10}, {max_overflow, 30}],
                [],
                [
                    [{port, 6380}],
                    [{port, 6379}],
                    [{port, 6381}],
                    [{port, 6382}],
                    [{port, 6383}],
                    [{port, 6384}]
                ]}}
        ]}
```
### 参数
mode 工作模式 
* cluster cluster集群模式
* single  单机模式



### Build
-----

    $ rebar3 compile
