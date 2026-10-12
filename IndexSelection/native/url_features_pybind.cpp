#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py = pybind11;

namespace {

constexpr std::size_t kDomainFreqFeatureCount = 2;
constexpr std::size_t kBaseFeatureCount = 18;
constexpr std::size_t kFeatureCount = 56;
constexpr std::size_t kTldFeatureStartIndex = kDomainFreqFeatureCount + kBaseFeatureCount;
constexpr std::size_t kTldOtherFeatureIndex = kFeatureCount - 1;
constexpr float kMaxUrlLen = 2048.0f;
constexpr float kMaxPathLen = 1024.0f;
constexpr float kMaxQueryLen = 1024.0f;
constexpr float kMaxPathDepth = 32.0f;
constexpr float kMaxNumParams = 32.0f;
constexpr float kMaxNumDigits = 512.0f;
constexpr float kMaxNumHyphen = 64.0f;
constexpr float kMaxNumUnderscore = 64.0f;
constexpr float kMaxDomainLen = 64.0f;
constexpr float kMaxSubdomainCount = 8.0f;
constexpr float kMaxTldLen = 16.0f;
constexpr float kMaxFileExtLen = 8.0f;

const std::vector<std::u32string> kTldCols = {
    U"ac.uk", U"au", U"br", U"ca", U"cn", U"co", U"co.jp", U"co.uk", U"com", U"com.au",
    U"com.br", U"com.cn", U"com.tw", U"de", U"edu", U"es", U"fr", U"gov", U"gov.uk", U"info",
    U"io", U"it", U"jp", U"kr", U"ne.jp", U"net", U"net.au", U"or.jp", U"org", U"org.au",
    U"org.uk", U"ru", U"tw", U"uk", U"us",
};

const std::vector<std::u32string> kMultiTlds = {
    U"co.uk", U"org.uk", U"ac.uk", U"gov.uk",
    U"co.jp", U"ne.jp", U"or.jp",
    U"com.au", U"net.au", U"org.au",
    U"com.br", U"com.cn", U"com.tw",
};

struct U32Hash {
    std::size_t operator()(const std::u32string& value) const noexcept {
        std::size_t hash = 1469598103934665603ULL;
        for (char32_t ch : value) {
            hash ^= static_cast<std::uint32_t>(ch);
            hash *= 1099511628211ULL;
        }
        return hash;
    }
};

bool is_ascii_alpha(char32_t ch) {
    return (ch >= U'a' && ch <= U'z') || (ch >= U'A' && ch <= U'Z');
}

bool is_ascii_digit(char32_t ch) {
    return ch >= U'0' && ch <= U'9';
}

bool is_ascii_alnum(char32_t ch) {
    return is_ascii_alpha(ch) || is_ascii_digit(ch);
}

char32_t ascii_lower(char32_t ch) {
    if (ch >= U'A' && ch <= U'Z') {
        return ch - U'A' + U'a';
    }
    return ch;
}

float clip_float(float value, float max_value) {
    return value > max_value ? max_value : value;
}

bool is_ascii_whitespace(char32_t ch) {
    return ch == U' ' || ch == U'\t' || ch == U'\n' || ch == U'\r' || ch == U'\f' || ch == U'\v';
}

bool is_word_char(char32_t ch) {
    if (ch == U'_') {
        return true;
    }
    if (ch > 127) {
        return true;
    }
    return is_ascii_alnum(ch);
}

std::u32string trim_ascii_whitespace(const std::u32string& value) {
    std::size_t start = 0;
    std::size_t end = value.size();
    while (start < end && is_ascii_whitespace(value[start])) {
        ++start;
    }
    while (end > start && is_ascii_whitespace(value[end - 1])) {
        --end;
    }
    return value.substr(start, end - start);
}

std::size_t find_char(std::u32string_view text, char32_t needle, std::size_t start = 0) {
    for (std::size_t idx = start; idx < text.size(); ++idx) {
        if (text[idx] == needle) {
            return idx;
        }
    }
    return std::u32string_view::npos;
}

std::size_t find_substring(std::u32string_view text, std::u32string_view needle) {
    if (needle.empty() || needle.size() > text.size()) {
        return std::u32string_view::npos;
    }
    const std::size_t limit = text.size() - needle.size();
    for (std::size_t idx = 0; idx <= limit; ++idx) {
        bool matched = true;
        for (std::size_t offset = 0; offset < needle.size(); ++offset) {
            if (text[idx + offset] != needle[offset]) {
                matched = false;
                break;
            }
        }
        if (matched) {
            return idx;
        }
    }
    return std::u32string_view::npos;
}

bool starts_with(std::u32string_view text, std::u32string_view prefix) {
    if (prefix.size() > text.size()) {
        return false;
    }
    for (std::size_t idx = 0; idx < prefix.size(); ++idx) {
        if (text[idx] != prefix[idx]) {
            return false;
        }
    }
    return true;
}

bool is_valid_url_scheme(std::u32string_view value) {
    if (value.empty() || !is_ascii_alpha(value[0])) {
        return false;
    }
    for (char32_t ch : value) {
        if (is_ascii_alnum(ch) || ch == U'+' || ch == U'-' || ch == U'.') {
            continue;
        }
        return false;
    }
    return true;
}

std::u32string normalize_netloc(std::u32string_view netloc) {
    if (netloc.empty()) {
        return {};
    }

    std::u32string normalized;
    normalized.reserve(netloc.size());
    for (char32_t ch : netloc) {
        normalized.push_back(ascii_lower(ch));
    }

    if (starts_with(normalized, U"www.")) {
        normalized.erase(0, 4);
    }

    const std::size_t port_sep = find_char(normalized, U':');
    if (port_sep != std::u32string::npos) {
        normalized.resize(port_sep);
    }
    return normalized;
}

int count_non_empty_segments(std::u32string_view text, char32_t separator) {
    if (text.empty()) {
        return 0;
    }
    int count = 0;
    bool in_segment = false;
    for (char32_t ch : text) {
        if (ch == separator) {
            if (in_segment) {
                ++count;
                in_segment = false;
            }
        } else if (!in_segment) {
            in_segment = true;
        }
    }
    if (in_segment) {
        ++count;
    }
    return count;
}

struct CharacterCounts {
    int digits = 0;
    int hyphen = 0;
    int underscore = 0;
};

CharacterCounts count_url_characters(std::u32string_view text) {
    CharacterCounts counts;
    for (char32_t ch : text) {
        if (is_ascii_digit(ch)) {
            ++counts.digits;
        } else if (ch == U'-') {
            ++counts.hyphen;
        } else if (ch == U'_') {
            ++counts.underscore;
        }
    }
    return counts;
}

bool boundary_ok(std::u32string_view text, std::size_t start, std::size_t end) {
    if (start > 0 && is_word_char(text[start - 1])) {
        return false;
    }
    if (end < text.size() && is_word_char(text[end])) {
        return false;
    }
    return true;
}

bool has_n_digits(std::u32string_view text, std::size_t start, int min_digits, int max_digits, std::size_t* end_out) {
    int count = 0;
    std::size_t idx = start;
    while (idx < text.size() && count < max_digits && is_ascii_digit(text[idx])) {
        ++idx;
        ++count;
    }
    if (count < min_digits) {
        return false;
    }
    *end_out = idx;
    return true;
}

bool contains_date_pattern(std::u32string_view text) {
    if (text.empty()) {
        return false;
    }

    for (std::size_t idx = 0; idx < text.size(); ++idx) {
        if (!is_ascii_digit(text[idx])) {
            continue;
        }

        if (
            idx + 4 <= text.size() &&
            ((text[idx] == U'1' && text[idx + 1] == U'9') || (text[idx] == U'2' && text[idx + 1] == U'0')) &&
            is_ascii_digit(text[idx + 2]) &&
            is_ascii_digit(text[idx + 3]) &&
            boundary_ok(text, idx, idx + 4)
        ) {
            return true;
        }

        std::size_t first_end = 0;
        if (!has_n_digits(text, idx, 4, 4, &first_end)) {
            first_end = idx;
        } else if (first_end < text.size() && (text[first_end] == U'-' || text[first_end] == U'/')) {
            const char32_t sep = text[first_end];
            std::size_t second_end = 0;
            if (
                has_n_digits(text, first_end + 1, 1, 2, &second_end) &&
                second_end < text.size() &&
                text[second_end] == sep
            ) {
                std::size_t third_end = 0;
                if (has_n_digits(text, second_end + 1, 1, 2, &third_end) && boundary_ok(text, idx, third_end)) {
                    return true;
                }
            }
        }

        if (!has_n_digits(text, idx, 1, 2, &first_end)) {
            continue;
        }
        if (first_end >= text.size() || (text[first_end] != U'-' && text[first_end] != U'/')) {
            continue;
        }
        const char32_t sep = text[first_end];
        std::size_t second_end = 0;
        if (!has_n_digits(text, first_end + 1, 1, 2, &second_end)) {
            continue;
        }
        if (second_end >= text.size() || text[second_end] != sep) {
            continue;
        }
        std::size_t third_end = 0;
        if (has_n_digits(text, second_end + 1, 2, 4, &third_end) && boundary_ok(text, idx, third_end)) {
            return true;
        }
    }
    return false;
}

struct SplitUrl {
    std::u32string raw;
    std::u32string scheme;
    std::u32string domain;
    std::u32string path;
    std::u32string query;
    std::u32string fragment;
};

SplitUrl split_url_fast(const std::u32string& url) {
    SplitUrl result;
    result.raw = trim_ascii_whitespace(url);
    if (result.raw.empty()) {
        return result;
    }

    const std::u32string_view raw_view(result.raw);
    const std::size_t scheme_sep = find_substring(raw_view, U"://");
    if (scheme_sep != std::u32string_view::npos && scheme_sep > 0 && is_valid_url_scheme(raw_view.substr(0, scheme_sep))) {
        std::u32string_view scheme_view = raw_view.substr(0, scheme_sep);
        result.scheme.reserve(scheme_view.size());
        for (char32_t ch : scheme_view) {
            result.scheme.push_back(ascii_lower(ch));
        }

        const std::size_t remainder_start = scheme_sep + 3;
        std::u32string_view remainder = raw_view.substr(remainder_start);

        std::size_t fragment_sep = find_char(remainder, U'#');
        if (fragment_sep != std::u32string_view::npos) {
            result.fragment = std::u32string(remainder.substr(fragment_sep + 1));
            remainder = remainder.substr(0, fragment_sep);
        }

        std::size_t query_sep = find_char(remainder, U'?');
        if (query_sep != std::u32string_view::npos) {
            result.query = std::u32string(remainder.substr(query_sep + 1));
            remainder = remainder.substr(0, query_sep);
        }

        std::size_t path_sep = find_char(remainder, U'/');
        if (path_sep != std::u32string_view::npos) {
            result.domain = normalize_netloc(remainder.substr(0, path_sep));
            result.path = std::u32string(remainder.substr(path_sep));
        } else {
            result.domain = normalize_netloc(remainder);
        }
        return result;
    }

    std::u32string_view remainder = raw_view;
    std::size_t fragment_sep = find_char(remainder, U'#');
    if (fragment_sep != std::u32string_view::npos) {
        result.fragment = std::u32string(remainder.substr(fragment_sep + 1));
        remainder = remainder.substr(0, fragment_sep);
    }

    std::size_t query_sep = find_char(remainder, U'?');
    if (query_sep != std::u32string_view::npos) {
        result.query = std::u32string(remainder.substr(query_sep + 1));
        remainder = remainder.substr(0, query_sep);
    }

    result.path = std::u32string(remainder);
    std::size_t path_sep = find_char(remainder, U'/');
    std::u32string_view domain_guess = path_sep == std::u32string_view::npos ? remainder : remainder.substr(0, path_sep);
    if (!domain_guess.empty() && domain_guess[0] == U'/') {
        domain_guess = std::u32string_view();
    }
    result.domain = normalize_netloc(domain_guess);
    return result;
}

std::vector<std::u32string_view> split_non_empty_parts(std::u32string_view text, char32_t separator) {
    std::vector<std::u32string_view> parts;
    std::size_t start = 0;
    for (std::size_t idx = 0; idx <= text.size(); ++idx) {
        if (idx == text.size() || text[idx] == separator) {
            if (idx > start) {
                parts.emplace_back(text.data() + start, idx - start);
            }
            start = idx + 1;
        }
    }
    return parts;
}

std::size_t find_tld_feature_index(std::u32string_view tld) {
    for (std::size_t idx = 0; idx < kTldCols.size(); ++idx) {
        if (kTldCols[idx] == tld) {
            return kTldFeatureStartIndex + idx;
        }
    }
    return kTldOtherFeatureIndex;
}

std::u32string extract_tld(std::u32string_view domain, std::size_t* tld_len_out, std::size_t* subdomain_count_out) {
    if (domain.empty()) {
        *tld_len_out = 0;
        *subdomain_count_out = 0;
        return U"other";
    }

    std::vector<std::u32string_view> raw_parts;
    std::size_t raw_start = 0;
    for (std::size_t idx = 0; idx <= domain.size(); ++idx) {
        if (idx == domain.size() || domain[idx] == U'.') {
            raw_parts.emplace_back(domain.data() + raw_start, idx - raw_start);
            raw_start = idx + 1;
        }
    }

    auto parts = split_non_empty_parts(domain, U'.');
    if (raw_parts.empty() || parts.empty()) {
        *tld_len_out = 0;
        *subdomain_count_out = 0;
        return U"other";
    }

    *tld_len_out = parts.back().size();
    *subdomain_count_out = parts.size() > 2 ? parts.size() - 2 : 0;
    if (raw_parts.size() >= 2) {
        std::u32string candidate;
        candidate.reserve(raw_parts[raw_parts.size() - 2].size() + 1 + raw_parts.back().size());
        candidate.append(raw_parts[raw_parts.size() - 2]);
        candidate.push_back(U'.');
        candidate.append(raw_parts.back());
        for (const auto& multi_tld : kMultiTlds) {
            if (candidate == multi_tld) {
                return candidate;
            }
        }
        return std::u32string(raw_parts.back());
    }
    return U"other";
}

std::size_t extract_file_extension_len(std::u32string_view path) {
    if (path.empty()) {
        return 0;
    }
    std::size_t tail_start = 0;
    const std::size_t last_slash = path.find_last_of(U'/');
    if (last_slash != std::u32string_view::npos) {
        tail_start = last_slash + 1;
    }
    std::u32string_view tail = path.substr(tail_start);
    const std::size_t last_dot = tail.find_last_of(U'.');
    if (last_dot == std::u32string_view::npos || last_dot + 1 >= tail.size()) {
        return 0;
    }
    const std::size_t ext_len = tail.size() - last_dot - 1;
    if (ext_len > 8) {
        return 0;
    }
    return ext_len;
}

class DomainFreqLookup {
public:
    DomainFreqLookup() = default;

    DomainFreqLookup(const py::dict& label_1_domain_freq, const py::dict& label_0_domain_freq) {
        label_1_freq_.reserve(label_1_domain_freq.size());
        for (const auto& item : label_1_domain_freq) {
            std::u32string key = py::cast<std::u32string>(item.first);
            const int value = py::cast<int>(item.second);
            label_1_freq_[std::move(key)] = value;
        }
        label_0_freq_.reserve(label_0_domain_freq.size());
        for (const auto& item : label_0_domain_freq) {
            std::u32string key = py::cast<std::u32string>(item.first);
            const int value = py::cast<int>(item.second);
            label_0_freq_[std::move(key)] = value;
        }
    }

    std::pair<int, int> lookup(const std::u32string& domain) const {
        return {
            lookup_single(label_1_freq_, domain),
            lookup_single(label_0_freq_, domain),
        };
    }

private:
    static int lookup_single(
        const std::unordered_map<std::u32string, int, U32Hash>& freq,
        const std::u32string& domain
    ) {
        const auto it = freq.find(domain);
        if (it == freq.end()) {
            return 0;
        }
        return it->second;
    }

    std::unordered_map<std::u32string, int, U32Hash> label_1_freq_;
    std::unordered_map<std::u32string, int, U32Hash> label_0_freq_;
};

void fill_feature_row(const std::u32string& url, const DomainFreqLookup& domain_freq, float* row) {
    for (std::size_t idx = 0; idx < kFeatureCount; ++idx) {
        row[idx] = 0.0f;
    }

    SplitUrl parts = split_url_fast(url);
    if (parts.raw.empty()) {
        return;
    }

    const float url_len = static_cast<float>(parts.raw.size());
    const float path_len = static_cast<float>(parts.path.size());
    const float query_len = static_cast<float>(parts.query.size());
    const int path_depth = count_non_empty_segments(parts.path, U'/');
    const float has_query = parts.query.empty() ? 0.0f : 1.0f;
    const int num_params = count_non_empty_segments(parts.query, U'&');
    const float has_fragment = parts.fragment.empty() ? 0.0f : 1.0f;
    const float https = parts.scheme == U"https" ? 1.0f : 0.0f;
    const float is_homepage = (parts.path.empty() || parts.path == U"/") ? 1.0f : 0.0f;
    const CharacterCounts counts = count_url_characters(parts.raw);
    const float domain_len = static_cast<float>(parts.domain.size());
    std::size_t tld_len = 0;
    std::size_t subdomain_count = 0;
    const std::u32string tld = extract_tld(parts.domain, &tld_len, &subdomain_count);
    const std::size_t file_ext_len = extract_file_extension_len(parts.path);
    const float has_date = (contains_date_pattern(parts.path) || contains_date_pattern(parts.query)) ? 1.0f : 0.0f;
    const auto domain_freq_pair = domain_freq.lookup(parts.domain);

    row[0] = static_cast<float>(domain_freq_pair.first);
    row[1] = static_cast<float>(domain_freq_pair.second);
    row[2] = clip_float(url_len, kMaxUrlLen);
    row[3] = clip_float(path_len, kMaxPathLen);
    row[4] = clip_float(query_len, kMaxQueryLen);
    row[5] = clip_float(static_cast<float>(path_depth), kMaxPathDepth);
    row[6] = has_query;
    row[7] = clip_float(static_cast<float>(num_params), kMaxNumParams);
    row[8] = has_fragment;
    row[9] = https;
    row[10] = is_homepage;
    row[11] = clip_float(static_cast<float>(counts.digits), kMaxNumDigits);
    row[12] = url_len > 0 ? static_cast<float>(counts.digits) / url_len : 0.0f;
    row[13] = clip_float(static_cast<float>(counts.hyphen), kMaxNumHyphen);
    row[14] = clip_float(static_cast<float>(counts.underscore), kMaxNumUnderscore);
    row[15] = clip_float(domain_len, kMaxDomainLen);
    row[16] = clip_float(static_cast<float>(subdomain_count), kMaxSubdomainCount);
    row[17] = clip_float(static_cast<float>(tld_len), kMaxTldLen);
    row[18] = clip_float(static_cast<float>(file_ext_len), kMaxFileExtLen);
    row[19] = has_date;
    row[find_tld_feature_index(tld)] = 1.0f;
}

py::list tld_cols() {
    py::list result;
    for (const auto& tld : kTldCols) {
        result.append(py::cast(tld));
    }
    return result;
}

py::list extract_domains(const py::sequence& urls) {
    py::list result;
    for (const auto& item : urls) {
        std::u32string url = py::cast<std::u32string>(item);
        SplitUrl parsed = split_url_fast(url);
        result.append(py::cast(parsed.domain));
    }
    return result;
}

py::array_t<uint32_t> extract_domain_hashes(const py::sequence& urls) {
    std::vector<std::u32string> url_buffer;
    url_buffer.reserve(urls.size());
    for (const auto& item : urls) {
        url_buffer.push_back(py::cast<std::u32string>(item));
    }
    
    py::array_t<uint32_t> hashes(url_buffer.size());
    auto mutable_hashes = hashes.mutable_unchecked<1>();
    
    for (size_t i = 0; i < url_buffer.size(); ++i) {
        SplitUrl parsed = split_url_fast(url_buffer[i]);
        std::size_t h = 1469598103934665603ULL;
        for (char32_t ch : parsed.domain) {
            h ^= static_cast<std::uint32_t>(ch);
            h *= 1099511628211ULL;
        }
        mutable_hashes(i) = static_cast<uint32_t>(h);
    }
    
    return hashes;
}

py::array_t<float> build_feature_matrix_native(
    const py::sequence& urls,
    const DomainFreqLookup& domain_freq,
    const std::string& progress_label = "",
    std::size_t progress_interval = 0
) {
    std::vector<std::u32string> url_buffer;
    url_buffer.reserve(urls.size());
    for (const auto& item : urls) {
        url_buffer.push_back(py::cast<std::u32string>(item));
    }

    py::array_t<float> matrix({static_cast<py::ssize_t>(url_buffer.size()), static_cast<py::ssize_t>(kFeatureCount)});
    auto mutable_matrix = matrix.mutable_unchecked<2>();

    const bool should_print_progress = progress_interval > 0 && !progress_label.empty();
    if (!should_print_progress) {
        py::gil_scoped_release release;
        for (py::ssize_t row_idx = 0; row_idx < mutable_matrix.shape(0); ++row_idx) {
            fill_feature_row(url_buffer[static_cast<std::size_t>(row_idx)], domain_freq, &mutable_matrix(row_idx, 0));
        }
        return matrix;
    }

    for (py::ssize_t row_idx = 0; row_idx < mutable_matrix.shape(0); ++row_idx) {
        fill_feature_row(url_buffer[static_cast<std::size_t>(row_idx)], domain_freq, &mutable_matrix(row_idx, 0));
        const std::size_t built = static_cast<std::size_t>(row_idx) + 1;
        if (built % progress_interval == 0) {
            py::print(progress_label + "_features_built=" + std::to_string(built), py::arg("flush") = true);
        }
    }

    return matrix;
}

}  // namespace

PYBIND11_MODULE(_url_features_native, m) {
    m.doc() = "Native URL feature extraction helpers for IndexSelection_v1";

    py::class_<DomainFreqLookup>(m, "DomainFreqLookup")
        .def(py::init<const py::dict&, const py::dict&>());

    m.def("build_feature_matrix_native", &build_feature_matrix_native, py::arg("urls"), py::arg("domain_freq"), py::arg("progress_label") = "", py::arg("progress_interval") = 0);
    m.def("feature_count", []() { return kFeatureCount; });
    m.def("tld_cols", &tld_cols);
    m.def("extract_domains", &extract_domains, py::arg("urls"));
    m.def("extract_domain_hashes", &extract_domain_hashes, py::arg("urls"));
}
