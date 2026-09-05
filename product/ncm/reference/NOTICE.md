# Third-party notice — Biomem reference

The Rust NCM backend (`crates/tracedecay-memory-ncm-core`, `crates/tracedecay-memory-ncm-runtime`)
ports algorithm structure from **Biomem** (`BleedingDev/biomem`, commit
`500847ff65b5d9548b3826fa29bf3ccf8d221147`), which is distributed under the MIT License:

```
MIT License

Copyright (c) 2026 biomem contributors

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
associated documentation files (the "Software"), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute,
sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or
substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT
OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```

No CC BY-NC supplementary material (BioCortexAI documentation) is copied into product artifacts.
The embedding model `paraphrase-multilingual-MiniLM-L12-v2` is Apache-2.0 (sentence-transformers);
the ONNX export is loaded from the `Xenova` mirror and pinned by digest in `embedding-manifest.json`.
Python remains a reference oracle only and is never shipped.
