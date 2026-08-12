# LinkedIn post draft: Automated parameter extraction from astronomical PDFs

For the last phase of this project, I focused on building an extraction pipeline that can pull physical parameters for eclipsing/contact binary systems directly from scientific PDFs.

The interesting part is that a “simple PDF parser” is not enough: papers vary wildly in notation, table layout, and PDF quality. Some documents are text-based, others are image-based scans, and OCR can introduce subtle token errors that break naive regex approaches.

To handle this, the pipeline combines two complementary strategies:

1. **Regex-based extraction** for high-precision parsing of parameter patterns (with normalization and uncertainty handling).
2. **Embeddings-based semantic retrieval** to find the most relevant text/table context when the formatting doesn’t match our regex expectations.

We also route text vs image PDFs differently (OCR when needed), and use a multi-stage relevance classifier so we don’t waste time on papers that only mention an object in passing.

My opinion: AI agents can dramatically simplify coverage of the “long tail” of regular-expression variants and document formats. But fully automating scientific extraction is still hard because the underlying data is not unified—publishers and authors choose different conventions, and there’s no universal schema to lean on.

If you’re working on similar pipelines, I’d love to compare notes on where you’ve found the biggest automation bottlenecks.

