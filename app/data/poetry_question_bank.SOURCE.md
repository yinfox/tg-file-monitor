# Poetry Question Bank Source

The generated `poetry_question_bank.sqlite` file is derived from the
`chinese-poetry/chinese-poetry` JSON corpus:

https://github.com/chinese-poetry/chinese-poetry

Included source directories:

- `五代诗词`
- `元曲`
- `全唐诗`
- `四书五经`
- `宋词`
- `幽梦影`
- `御定全唐詩`
- `曹操诗集`
- `楚辞`
- `水墨唐诗`
- `纳兰性德`
- `蒙学`
- `论语`
- `诗经`

Generation command:

```bash
.venv/bin/python scripts/build_poetry_question_bank.py \
  --source /tmp/chinese-poetry \
  --output app/data/poetry_question_bank.sqlite
```

The source project is distributed under the MIT License:

Copyright (c) 2016 JackeyGao

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
