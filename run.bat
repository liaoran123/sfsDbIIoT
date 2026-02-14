@echo off

REM 智能工厂设备监控系统启动脚本

setlocal

REM 检查 Go 是否安装
where go >nul 2>nul
if %errorlevel% neq 0 (
    echo 错误: 未找到 Go 命令，请确保 Go 已安装并添加到 PATH 环境变量
    pause
    exit /b 1
)

REM 检查依赖
if not exist "go.mod" (
    echo 错误: 未找到 go.mod 文件，请确保在正确的目录中运行
    pause
    exit /b 1
)

REM 下载依赖
echo 正在下载依赖...
go mod tidy
if %errorlevel% neq 0 (
    echo 错误: 下载依赖失败
    pause
    exit /b 1
)

REM 构建应用
echo 正在构建应用...
go build -o sfsDbIIoT.exe
if %errorlevel% neq 0 (
    echo 错误: 构建失败
    pause
    exit /b 1
)

REM 创建数据目录
if not exist "data" (
    echo 创建数据目录...
    mkdir data
)

REM 启动应用
echo 正在启动智能工厂设备监控系统...
echo 按 Ctrl+C 退出系统
echo.

sfsDbIIoT.exe

endlocal
