#!/usr/bin/env bash
# dbmw 在 WSL / Ubuntu 上的一键初始化脚本：安装依赖 + 配置 + 构建。
# 用法：
#   ./scripts/setup-wsl.sh                 # 仅核心层
#   ./scripts/setup-wsl.sh --mysql         # 启用 MySQL 驱动
#   ./scripts/setup-wsl.sh --mysql --pg --odbc
set -euo pipefail

ENABLE_MYSQL=OFF
ENABLE_POSTGRES=OFF
ENABLE_ODBC=OFF

for arg in "$@"; do
  case "$arg" in
    --mysql) ENABLE_MYSQL=ON ;;
    --pg|--postgres) ENABLE_POSTGRES=ON ;;
    --odbc) ENABLE_ODBC=ON ;;
    *) echo "未知参数: $arg" >&2; exit 1 ;;
  esac
done

echo "==> 更新 apt 并安装基础工具链"
sudo apt update
sudo apt install -y build-essential cmake

if [ "$ENABLE_MYSQL" = ON ]; then
  echo "==> 安装 MySQL 客户端库"
  sudo apt install -y default-libmysqlclient-dev
fi
if [ "$ENABLE_POSTGRES" = ON ]; then
  echo "==> 安装 PostgreSQL 客户端库"
  sudo apt install -y libpqxx-dev libpq-dev
fi
if [ "$ENABLE_ODBC" = ON ]; then
  echo "==> 安装 unixODBC 开发库"
  sudo apt install -y unixodbc-dev
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD="$ROOT/build"
echo "==> 配置构建 (mysql=$ENABLE_MYSQL pg=$ENABLE_POSTGRES odbc=$ENABLE_ODBC)"
mkdir -p "$BUILD"
cmake -S "$ROOT" -B "$BUILD" \
  -DDBMW_ENABLE_MYSQL="$ENABLE_MYSQL" \
  -DDBMW_ENABLE_POSTGRES="$ENABLE_POSTGRES" \
  -DDBMW_ENABLE_ODBC="$ENABLE_ODBC"

echo "==> 编译"
cmake --build "$BUILD" -j"$(nproc)"

echo "==> 完成。运行示例："
echo "    $BUILD/examples/dbmw_example_basic $ROOT/config/datasources.json.example"
