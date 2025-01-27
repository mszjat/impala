// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/char-codec.h"

#include <boost/locale.hpp>
#include <string>

#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "gutil/strings/substitute.h"

using namespace impala;
using namespace strings;

CharCodec::CharCodec(MemPool* memory_pool, const std::string& encoding, char tuple_delim)
    : memory_pool_(memory_pool), encoding_(encoding), tuple_delim_(tuple_delim) {
}

const int MAX_SYMBOL = 4;

Status CharCodec::DecodeBuffer(uint8_t** buffer, int64_t* bytes_read) {
  std::string result_prefix;
  std::string result_core;
  std::string result_suffix;

  uint8_t* buf_start = *buffer;
  uint8_t* buf_end = buf_start + *bytes_read;

  if (!partial_symbol_.empty()) {
    std::vector<uint8_t> prefix = partial_symbol_;
    bool success = false;

    for (int i = 0; partial_symbol_.size() + i < MAX_SYMBOL
        && buf_start + i < buf_end; ++i) {
      prefix.push_back(buf_start[i]);
      try {
        result_prefix = boost::locale::conv::to_utf<char>(
            (char*)prefix.data(),
            (char*)prefix.data() + prefix.size(),
            encoding_, boost::locale::conv::stop);
        success = true;
        buf_start += i + 1;
        break;
      } catch (boost::locale::conv::conversion_error&) {
        // continue
      } catch (const std::exception& e) {
        return Status(TErrorCode::CHARSET_CONVERSION_ERROR, e.what());
      }
    }

    if (!success) {
      return Status(TErrorCode::CHARSET_CONVERSION_ERROR, "Unable to decode buffer");
    }
  }

  partial_symbol_.clear();

  uint8_t* last_delim =
      std::find_end(buf_start, buf_end, &tuple_delim_, &tuple_delim_ + 1);
  if (last_delim != buf_end) {
    try {
      result_core = boost::locale::conv::to_utf<char>(
          (char*)buf_start, (char*)last_delim + 1, encoding_, boost::locale::conv::stop);
    } catch (boost::locale::conv::conversion_error& e) {
      return Status(TErrorCode::CHARSET_CONVERSION_ERROR, e.what());
    } catch (const std::exception& e) {
      return Status(TErrorCode::CHARSET_CONVERSION_ERROR, e.what());
    }

    buf_start = last_delim + 1;
  }

  if (buf_start < buf_end) {
    bool success = false;

    uint8_t* end = buf_end;
    while (buf_end - end < MAX_SYMBOL && end > buf_start) {
      try {
        result_suffix = boost::locale::conv::to_utf<char>(
            (char*)buf_start, (char*)end,
            encoding_, boost::locale::conv::stop);
        success = true;
        break;
      } catch (boost::locale::conv::conversion_error&) {
        --end;
      } catch (const std::exception& e) {
        return Status(TErrorCode::CHARSET_CONVERSION_ERROR, e.what());
      }
    }

    if (!success) {
      return Status(TErrorCode::CHARSET_CONVERSION_ERROR, "Unable to decode buffer");
    }
    if (end < buf_end) {
      partial_symbol_.assign(end, buf_end);
    }
  }

  *bytes_read = result_prefix.size() + result_core.size() + result_suffix.size();
  *buffer = (uint8_t*)memory_pool_->TryAllocate(*bytes_read);
  if (UNLIKELY(*buffer == nullptr)) {
      string details = Substitute(
          "HdfsTextScanner::DecodeBuffer() failed to allocate $1 bytes.",
          *bytes_read);
      return memory_pool_->mem_tracker()->MemLimitExceeded(
          nullptr, details, *bytes_read);
  }
  memcpy(*buffer, result_prefix.data(), result_prefix.size());
  memcpy(*buffer + result_prefix.size(), result_core.data(), result_core.size());
  memcpy(*buffer + result_prefix.size() + result_core.size(),
      result_suffix.data(), result_suffix.size());

  return Status::OK();
}

Status CharCodec::EncodeBuffer(const std::string& str, std::string& result) {
  try {
    result = boost::locale::conv::from_utf<char>(
        str, encoding_, boost::locale::conv::stop);
  } catch (boost::locale::conv::conversion_error& e) {
    return Status(TErrorCode::CHARSET_CONVERSION_ERROR, e.what());
  } catch (const std::exception& e) {
    return Status(TErrorCode::CHARSET_CONVERSION_ERROR, e.what());
  }

  return Status::OK();
}
