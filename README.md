# KETI_Mobius_Contest
## 01. Installation Guide
**This Guide is based on macOS**
시작에 앞서 IoTKETI의 Mobius repository clone
```
% git clone https://github.com/IoTKETI/Mobius
```

IoTKETI의 mobius-open-platform repository clone
```
% git clone https://github.com/IoTKETI/mobius-open-platform
```

## 1-1. Mobius Server
### 1. MySQL Server
만약 이미 MySQL 서버가 설치 되어있다면 **반드시** 업그레이드 혹은 다운그레이드 해야 함. 만약 사용 중인 데이터베이스 존재 시 백업 완료 후 재설치 진행.
```
% mysql --version
mysql  Ver 8.4.6 for macos15.4 on arm64 (Homebrew)
```

mysql Ver 8.4 설치.
```
% brew install mysql@8.4
```

디렉터리 존재 및 소유권 확인.
```
% sudo mkdir -p /opt/homebrew/var/mysql
% sudo chown -R "$(whoami)":staff /opt/homebrew/var/mysql
```

MySQL 초기화.
```
% /opt/homebrew/opt/mysql@8.4/bin/mysqld --initialize-insecure \
  --user="$(whoami)" \
  --basedir="/opt/homebrew/opt/mysql@8.4" \
  --datadir="/opt/homebrew/var/mysql"
```

> 데이터 디렉터리에 이미 파일에 있다는 오류 발생 시 아래 명령 수행. (이전에 MySQL 설치가 되어 있었다면 발생 가능)
> 서버 완전 중지 및 잔재 데이터 제거
> ```
> % pkill -f mysqld || true
> % rm -f /opt/homebrew/var/mysql/mysql.sock /opt/homebrew/var/mysql/*.pid
> ```
> 
> 새 디렉터리 생성 및 권한 부여
> ```
> % sudo mkdir -p /opt/homebrew/var/mysql
> % sudo chown -R "$(whoami)":staff /opt/homebrew/var/mysql
> ```

필요 시 아래 명령 수행. PATH에 MySQL 8.4 바이너리 추가.
```
% echo 'export PATH="/opt/homebrew/opt/mysql@8.4/bin:$PATH"' >> ~/.zshrc
```

클라이언트 기본 소켓 경로 설정.
```
 % cat >> ~/.my.cnf <<'EOF'
[client]
socket=/opt/homebrew/var/mysql/mysql.sock
EOF
% chmod 600 ~/.my.cnf
```

설정 이후 shell 재시작 필수.
```
exec $SHELL -l
```

MySQL 서버 기동.
```
% brew services start mysql@8.4 
```

보안 설정 진행.
```
 % mysql_secure_installation
```

`root` 계정을 `mysql_native_password` 로 전환. MySQL 설정에서 해당 플러그인이 활성화 상태여야 함. 설정 파일에 접근.
```
nano /opt/homebrew/etc/my.cnf
```

해당 파일에 `mysql_native_password=ON` 존재하지 않을 시 `[mysqld]` 섹션 하단에 추가 이후 `Ctrl+O` > `Enter` > `Ctrl+X`.

설정 이후 MySQL 서버 재시작.
```
% brew services restart mysql@8.4
```

`root` plugin을 `mysql_native_password` 로 변경.
```
mysql -u root -p -e "ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'rr000628'; FLUSH PRIVILEGES;"
```

`mobiusdb`DB 생성.
```
% mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS mobiusdb CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;"
```

Mobius repository에서 다운로드 한 파일 중 `/mobius/mobiusdb.sql` 스키마 import.
```
% mysql -u root -p mobiusdb < "Mobius repository clone 경로/Mobius-master/mobius/mobiusdb.sql"
```

### 2. Mosquitto MQTT
mosquitto 설치.
```
% brew install mosquitto
```

mosquitto 실행.
```
% brew services start mosquitto
```

mosquitto MQTT broker 실행.
```
% mosquitto -v
```

### 3. Node.js
Node.js 공식 [website](https://nodejs.org/en/download)에 접속하여 다운로드 진행. website 접속 후 LTS 버전 선택, using `nvm`, with `npm` 선택 후 아래 안내되는 명령 수행.
```
// 예시
// Download and install nvm:
% curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.3/install.sh | bash

// in lieu of restarting the shell
% \. "$HOME/.nvm/nvm.sh"

// Download and install Node.js:
% nvm install 22

// Verify the Node.js version:
% node -v

// Verify npm version:
% npm -v
```

### 4. Mobius Server
`Mobius-master` 디렉토리에서 terminal 실행 이후 진행. 등록된 라이브러리 자동 설치.
```
% npm install
```

`Mobius-master` 디렉토리의 `conf.json` 파일 개인 설정에 맞게 수정.
```json
{
  "csebaseport": "7579", //Mobius HTTP hosting  port
  "dbpass": "*******"    //MySQL root password
}
```

Mobius 서버 실행.
```
% node mobius.js
```

## 1-2. Mobius resource browser
`mobius-open-platform` 디렉토리의 `resource_browser` 디렉토리로 이동 후 terminal 실행. `bower` 설치
```
% npm install bower -g
```

등록된 라이브러리 자동 설치
```
% npm install
```

`resource_browser` 내의 `public` 디렉토리로 이동
```
% cd public
```

`bower` 라이브러리 자동 설치
```
% bower install
```

상위 디렉토리로 이동 후 resource browser application 실행
```
% cd ..
% npm start
```

browser 창에서 [http://localhost:7575](http://localhost:7575) 주소로 접속
