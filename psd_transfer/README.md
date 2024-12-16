# PINGPONGのデータ取得

## データ転送
IREPのS3からnegociaのS3にデータを転送する。

### 準備
#### AWS Credentials
```
cp aws_credentials.sample aws_credentials
# CREDENTIALSを記入
```


#### Docker Build
```
docker build -t transfer .  
```

### ファイル転送の実行
#### 実行コマンド
```
docker run --rm transfer transfer
```

## PSDデータの取得
クレンジング情報の配置
- [Notion](https://www.notion.so/PSD-5f1fa40f90cf490faf65dc28b58601c6?pvs=4#60f0eee96f9f4af1b9f8c4aa5386da29)を参考に取得
- ```csv/cleansing.csv```に保存する

#### Docker Build
```
docker build -t select-transfer .
```

### ファイル転送の実行
#### 実行コマンド
```
docker run --rm select-transfer
```