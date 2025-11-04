@echo off
echo 启动行情中心...
start "行情中心" python hqCenter.py --token your_token_here --listen :31800

timeout /t 2 /nobreak >nul

echo 启动订单中心...
start "订单中心" python orderHolder.py --listen :31888

timeout /t 2 /nobreak >nul

echo 启动债券触发器...
start "债券触发器" python bondTrigger.py --listen :31866 --hqCenterAddr http://127.0.0.1:31800 --tdCenterAddr http://127.0.0.1:31888

echo 所有服务已启动完成
pause
