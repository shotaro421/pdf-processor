# PDF Structural Processor

PDFをGitHubにアップロードするだけで、自動的に構造化されたMarkdownに変換するサーバーレスパイプライン。

## 特徴

- **ゼロセットアップ**: GitHub Actionsで完全自動実行
- **大規模対応**: 2,300ページ級のドキュメントを処理可能
- **マルチLLM**: Gemini / OpenAI / Anthropic 自動切り替え
- **並列処理**: 月100件以上の大量処理に対応
- **コスト最適化**: Gemini 2.0 Flash で1ファイル数円〜

## クイックスタート

### 1. リポジトリをフォーク/クローン

```bash
git clone https://github.com/your-username/pdf-processor.git
cd pdf-processor
```

### 2. APIキーを設定

GitHubリポジトリの Settings > Secrets and variables > Actions で:

- `GOOGLE_API_KEY`: Google AI Studio APIキー
- `OPENAI_API_KEY`: (オプション) フォールバック用

### 3. PDFをアップロード

```bash
cp your-document.pdf input/
git add input/
git commit -m "Add PDF for processing"
git push
```

### 4. 結果を確認

処理完了後、`output/` フォルダに構造化Markdownが生成されます。

## プロジェクト構成

```
pdf-processor/
├── .github/workflows/   # GitHub Actions設定
│   └── process_pdf.yml
├── input/               # PDF入力フォルダ
├── output/              # 処理済みMarkdown出力
├── logs/                # 処理ログ・統計
├── prompts/             # LLMプロンプトテンプレート
│   ├── default.txt
│   ├── financial_report.txt
│   ├── earnings_report.txt
│   └── integrated_report.txt
├── scripts/             # 処理スクリプト
│   ├── processor.py    # メイン処理
│   ├── llm_client.py   # マルチLLMクライアント
│   ├── chunker.py      # チャンク分割
│   └── queue_manager.py # キュー管理
├── config.yaml          # 設定ファイル
└── requirements.txt
```

## 対応ドキュメント

| 種類 | プロンプト | 特徴 |
|------|-----------|------|
| 有価証券報告書 | financial_report.txt | 財務データ抽出に特化 |
| 決算短信 | earnings_report.txt | 業績ハイライト重視 |
| 統合報告書 | integrated_report.txt | ESG情報も構造化 |
| その他 | default.txt | 汎用的な構造化 |

## 設定カスタマイズ

config.yaml で以下を調整可能:

```yaml
# LLMモデルの変更
llm:
  primary:
    provider: "gemini"
    model: "gemini-2.0-flash"  # コスト重視
  secondary:
    provider: "gemini"
    model: "gemini-2.5-pro"    # 精度重視

# チャンク設定
chunking:
  max_tokens_per_chunk: 30000  # Geminiの大コンテキスト活用
  overlap_tokens: 500
```

## コスト目安

| モデル | 入力/1M tokens | 出力/1M tokens | 100ページPDF |
|--------|---------------|----------------|--------------|
| Gemini 2.0 Flash | $0.10 | $0.40 | 約$0.05 |
| Gemini 2.5 Pro | $1.25 | $5.00 | 約$0.50 |
| GPT-4o-mini | $0.15 | $0.60 | 約$0.08 |

## ローカル実行

```bash
pip install -r requirements.txt
export GOOGLE_API_KEY="your-api-key"
cd scripts && python processor.py
```

## ライセンス

MIT License
