## 取得済みデータ
[Notionページ](https://www.notion.so/126db36cc60180849852ce73f3728c1b)
- 実績の管理テーブル : https://www.notion.so/123db36cc60180d6833fcfe647220c2a?v=902e1648012849fcb06940ec0dc9fe26
- Creativeの管理テーブル : https://www.notion.so/15adb36cc60180dead18ce327b16d583?v=fa0e8b583be2486f84e998d20e21797d


## 実績とそれに紐づいたCreative(画像・動画)の取得手順
1. 実績取得
2. Creativeのダウンロード
3. csvの結合
4. サイズの付与
5. awsアップロード
6. 管理テーブルに追加



### 1. 実績取得
実績データをcsvとしてエクスポートするには、BigQueryで[query](./query/)から該当するクエリを実行する。クエリはirepまたはHTのアカウントを使用し、プロジェクトは```ida-prd```を選択する。

取得した実績はGCS(```gs://query_export_10days``` : 10日間で保存されたものが削除される)にエクスポートしてダウンロードする。命名は管理テーブルを参考に、```<media>_<image|video>_{table>_<開始年月>_<終了年月>.csv.gz```という形式にする場合が多い。
- サイズが大きい場合は保存されないので、```<media>_<image|video>_<hdy|xone>_<開始年月>_<終了年月>_*.csv.gz```と分割して保存する
- 分割したファイルは```poetry run python src/collect_csv.py [csvs_dir] [save_path]```で一つに集約できる。

### 2. Creativeのダウンロード
- awsからのダウンロード
    - negocia_aws_profile_nameとnegocia_s3_uriを入力することで、すでにs3に存在するものをダウンロード対象から除外
    - profileはdefaultに設定されているものは必要ない
 ```
poetry run python src/download_aws_file.py [csv_path] [save_dir] --irep_profile [irep_profile_name] --negocia_profile [negocia_aws_profile_name] --negocia_s3_uri [negocia_s3_uri]
 ```
- gcsからのダウンロード
    - negocia_aws_profile_nameとnegocia_s3_uriを入力することで、すでにs3に存在するものをダウンロード対象から除外
```
poetry run python src/download_gcs_file.py [csv_path] [save_dir] --negocia_profile [negocia_aws_profile_name]  --negocia_s3_uri [negocia_s3_uri]
```

### 3. csvの結合
hdyとxoneからそれぞれ取得している場合は結合して保存する。
```
poetry run python src/concat_with_priority.py [hdy_csv_path] [creative_report_csv_path] [save_path]
```
### 4. サイズの付与
画像・動画サイズはクエリからの情報が異なる場合があるので、実際のクリエイティブを参照し付与する
```
poetry run python src/overwrite_size.py [csv_path] [image_dir] [save_path] [image|video]
```

### 5. awsにアップロード
- 実績
```
aws s3 cp [csv_path] [s3_path]
```
- Creative
```
aws s3 sync [creative_dir] [s3_dir]
```

### 6. 管理テーブルに追加
Creativeで同媒体の場合は既存の場所に追加する
- 実績の管理テーブル : https://www.notion.so/123db36cc60180d6833fcfe647220c2a?v=902e1648012849fcb06940ec0dc9fe26
- Creativeの管理テーブル : https://www.notion.so/15adb36cc60180dead18ce327b16d583?v=fa0e8b583be2486f84e998d20e21797d

