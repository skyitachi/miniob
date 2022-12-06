/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by wangyunlai on 2022/9/28
//

#include <string.h>
#include <time.h>
#include <regex>
#include <string>
#include <vector>

#include "util/util.h"

static int days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
std::string double2string(double v)
{
  char buf[256];
  snprintf(buf, sizeof(buf), "%.2f", v);
  size_t len = strlen(buf);
  while (buf[len - 1] == '0') {
    len--;
      
  }
  if (buf[len - 1] == '.') {
    len--;
  }

  return std::string(buf, len);
}

std::string timestamp2string(int ts) {
  char buf[20];
  auto t = time_t(ts);
  auto len = strftime(buf, 11, "%Y-%m-%d", localtime(&t));
  return std::string(buf, len);
}

bool validate_date(const char *v) {
  std::string s = v;
  std::regex re("-");
  std::vector<std::string> parts(
      std::sregex_token_iterator(s.begin(), s.end(), re, -1),
      std::sregex_token_iterator());
  if (parts.size() != 3) {
    return false;
  }
  int y = std::stoi(parts[0]);
  int m = std::stoi(parts[1]);
  int d = std::stoi(parts[2]);
  if (m < 1 || m > 12 || d > 31 || d < 0) {
    return false;
  }
  bool is_leap = false;
  if (y % 4 == 0 && y % 100 != 0) {
    is_leap = true;
  }
  if (y % 400 == 0) {
    is_leap = true;
  }
  if (!is_leap && d >= 29) {
    return false;
  }
  if (d > days[m - 1]) {
    return false;
  }
  return true;
}