"""
MIT License

Copyright (c) 2019-present Josh B

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import decimal
import datetime
import ipaddress
import uuid

Integer = int
Int = int
SmallInt = int
BigInt = int
Serial = int
Float = float
DoublePercision = float
Double = float
Numeric = decimal.Decimal
# 8.2 Monetary
Money = str
# 8.3 Character
# CharacterVarying = VarChar = str
Character = str
Text = str
# 8.4 Binary
Bytea = bytes
# 8.5 Date/Time
Timestamp = datetime.datetime
TimestampAware = datetime.datetime
Date = datetime.date
Inteval = datetime.timedelta
# 8.6 Boolean
Boolean = bool
# 8.9 Network Adress
CIDR = ipaddress._BaseNetwork
Inet = ipaddress._BaseNetwork
MACAddr = str
# 8.12 UUID
UUID = uuid.UUID
# 8.14 JSON
JSON = dict
JSONB = dict
