# Batch Processing API

⚡ **Batch Call Any API with Parameter Substitution**

A powerful web application that enables batch API calls with dynamic parameter substitution. Load a list of parameters from a file, and execute the same API endpoint multiple times with different values for each call.

---

## 🎯 What It Does

The Batch Processing API allows you to call an API endpoint multiple times with different parameters without manual repetition. Useful for:
- Testing API endpoints with multiple scenarios
- Processing lists of IDs, emails, or other values
- Running migrations or batch operations
- Collecting responses from multiple API calls

**Example Use Cases:**
- Call `/api/users/{id}` for 1000 different user IDs
- Submit `/api/process` with different query parameters
- Perform bulk operations on a REST API
- Test API behavior across multiple input variations

---

## ✨ Features

### 🔗 **Batch API Calling**
- ✅ Call any HTTP endpoint multiple times
- ✅ Support for GET, POST, PUT, DELETE, PATCH methods
- ✅ Works with internal APIs (no CORS issues — proxy via Flask)
- ✅ Automatic parameter substitution
- ✅ Concurrent request execution for speed
- ✅ Detailed response tracking

### 📋 **Flexible Parameter Input**
- 📂 Upload parameter lists from CSV, JSON, or text files
- ✏️ **Manual entry** — Type parameters directly
- 📝 **File upload** — Load from local file
- 🔄 **Substitution** — `{param}` or `${param}` placeholders in URL
- 🎯 **Single value mode** — Use plain values when not using objects

### 🎮 **Smart Configuration**
- 🔐 **Authentication** — Support for API keys and auth headers
- ⏱️ **Timeout settings** — Control request timeouts
- 🔄 **Retry logic** — Automatic retry on failure
- 🎯 **Concurrent limit** — Control number of parallel requests
- 📊 **Batch size control** — Process in manageable chunks

### 📊 **Response Tracking & Analytics**
- ✅ Success/failure counts
- 📈 Response status codes
- ⏱️ Execution times per request
- 📝 Response preview and logs
- 💾 Export results to JSON/CSV

### 📥 **Multiple Output Formats**
- 📋 View results in table format
- 📥 Download complete response log
- 💾 Export to JSON for further processing
- 📊 View statistics and summary

---

## 🔧 How It Works

### Architecture

```
Browser (Web UI)
     ↓
Flask Server (localhost:5000)
     ↓
Target API Endpoint
     ↓
Response back to Browser
```

The Flask server acts as a proxy, eliminating CORS issues when calling external APIs from the browser.

### Step-by-Step Process

1. **Configure API**
   - Enter target API endpoint URL
   - Choose HTTP method (GET, POST, etc.)
   - Set authentication if needed

2. **Load Parameters**
   - Upload CSV/JSON with parameter list
   - OR paste parameter values
   - Specify parameter names for substitution

3. **Set Substitution Rules**
   - Define placeholders in URL (e.g., `/users/{id}`)
   - Map parameter names to placeholders
   - Support for path and query parameters

4. **Execute Batch**
   - Start batch processing
   - Concurrent requests sent to API
   - Progress tracking with real-time updates

5. **Review Results**
   - See success/failure counts
   - View individual responses
   - Export results for analysis

### Parameter Substitution

The tool supports multiple substitution patterns:

**Path parameter:**
```
URL: /api/users/{id}/posts
Parameters: { "id": "12345" }
Result: /api/users/12345/posts
```

**Query parameter:**
```
URL: /api/search?q={query}&limit=10
Parameters: { "query": "mongodb" }
Result: /api/search?q=mongodb&limit=10
```

**Multiple parameters:**
```
URL: /api/{resource}/{id}
Parameters: { "resource": "users", "id": "789" }
Result: /api/users/789
```

---

## 💻 Installation & Setup

### Requirements

- Python 3.7+
- Flask
- Requests library

### Installation

1. **Navigate to project directory:**
   ```bash
   cd batch-processing-api
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the server:**
   ```bash
   python batch_processing.py
   ```

4. **Open in browser:**
   ```
   http://127.0.0.1:5000
   ```

### Requirements File

```
flask>=2.3.0
requests>=2.31.0
```

---

## 🚀 Usage Guide

### Basic Workflow

1. **Open the web UI** — Navigate to `http://127.0.0.1:5000`

2. **Configure API endpoint:**
   - Enter target URL (e.g., `https://api.example.com/users/{id}`)
   - Select HTTP method
   - Add any required headers or authentication

3. **Prepare parameter list:**
   - Upload CSV: `id,name,email` format
   - Upload JSON: Array of objects
   - Or paste values directly

4. **Map parameters:**
   - Specify which fields map to URL placeholders
   - Configure query parameters if needed

5. **Execute batch:**
   - Click "Start Batch Processing"
   - Monitor progress in real-time
   - View results as they complete

6. **Review & Export:**
   - Check success/failure counts
   - View individual responses
   - Export results for downstream processing

### Input Formats

**CSV Format:**
```csv
id,name,email
1,John,john@example.com
2,Jane,jane@example.com
3,Bob,bob@example.com
```

**JSON Format:**
```json
[
  { "id": "1", "name": "John", "email": "john@example.com" },
  { "id": "2", "name": "Jane", "email": "jane@example.com" }
]
```

**Plain Text (single parameter):**
```
user@example.com
user2@example.com
user3@example.com
```

### Configuration Options

| Option | Description | Example |
|--------|-------------|---------|
| **API URL** | Target endpoint with placeholders | `/api/users/{id}` |
| **HTTP Method** | Request method | GET, POST, PUT, DELETE |
| **Auth Type** | Authentication method | None, Bearer, API Key |
| **Timeout** | Request timeout in seconds | 30 |
| **Concurrent** | Max parallel requests | 5-10 |
| **Retry Count** | Failed request retries | 0-3 |

---

## 🎯 Use Cases

### 1. **Bulk Data Processing**
- Process 1000+ records through an API
- Update multiple resources in one operation
- Example: Update all user profiles

### 2. **API Testing**
- Test endpoint with multiple parameter combinations
- Validate error handling
- Performance testing at scale

### 3. **Data Migration**
- Import data from CSV into API-backed system
- Migrate between databases
- Bulk create/update operations

### 4. **Report Generation**
- Fetch data for multiple entities
- Collect metrics from multiple endpoints
- Generate consolidated reports

### 5. **Monitoring & Health Checks**
- Call multiple endpoints to verify availability
- Collect status from distributed systems
- Log results for alerting

### 6. **Data Collection**
- Batch download from APIs
- Collect responses for analysis
- Export data for external tools

---

## 📊 Response Analysis

The tool provides detailed response tracking:

### Statistics Collected
- ✅ Total requests initiated
- ✅ Successful responses (2xx status)
- ✅ Failed responses (4xx, 5xx status)
- ⏱️ Average response time
- 📈 Min/max response times
- 📊 Response status distribution

### Response Export
- 📋 JSON format for processing
- 📊 CSV for spreadsheet analysis
- 📝 Full logs with timestamps
- 💾 Download for offline review

---

## 📈 Performance Specs

| Metric | Value |
|--------|-------|
| Max concurrent requests | Configurable (default: 5-10) |
| Max batch size | Depends on API and memory |
| Request timeout | Configurable (default: 30s) |
| Server port | 5000 (configurable) |
| Memory usage | Efficient streaming |

---

## 🛠️ Technical Stack

- **Backend:** Python with Flask
- **Frontend:** HTML/CSS/JavaScript (browser UI)
- **HTTP Client:** Python Requests library
- **Concurrency:** ThreadPoolExecutor for parallel requests
- **Database:** None (stateless design)

---

## 🌐 Supported APIs

Works with any HTTP API:
- ✅ REST APIs
- ✅ JSON APIs
- ✅ GraphQL (via POST)
- ✅ Custom endpoints
- ✅ Internal/private APIs (no CORS required)

---

## 📝 Configuration Examples

### Example 1: Fetch User Data
```
URL: https://api.example.com/users/{userId}
Method: GET
Parameters: [ "1", "2", "3", "4", "5" ]
Auth: Bearer token
```

### Example 2: Bulk Update Records
```
URL: https://api.example.com/records
Method: POST
Parameters: [
  { "id": "1", "status": "active" },
  { "id": "2", "status": "inactive" }
]
Body: { "status": "{status}" }
```

### Example 3: Search Multiple Terms
```
URL: https://search.example.com?q={query}&limit=10
Method: GET
Parameters: [ "mongodb", "python", "javascript" ]
```

---

## 🔒 Security Notes

- 🔐 **Server runs locally** — No data sent to external services
- 🔐 **API key storage** — Keep credentials secure
- 🔐 **HTTPS recommended** — Use for production APIs
- 🔐 **No data logging** — Responses not persisted by default

---

## 🐛 Troubleshooting

### "Connection refused" error
- Ensure Flask server is running: `python app.py`
- Check port 5000 is available
- Try accessing http://127.0.0.1:5000 directly

### API requests failing
- Verify target API endpoint is accessible
- Check authentication/API key is correct
- Verify parameter substitution is working correctly
- Check timeout settings (increase if needed)

### Large batch processing is slow
- This is normal for many requests
- Adjust concurrent request limit
- Consider batching into smaller groups
- Monitor target API rate limits

### CORS errors (original setup)
- Flask proxy eliminates CORS issues
- Ensure requests go through localhost:5000
- Don't bypass proxy with direct calls

---

## 📄 License

© 2026 All Rights Reserved.

This project is licensed under the **MIT License** — see [LICENSE](LICENSE) file for full details.

**You are free to:**
- ✅ Use this software for personal and commercial purposes
- ✅ Modify and adapt the code to your needs
- ✅ Distribute copies of the software
- ✅ Include it in your own projects

**You must:**
- 📋 Include a copy of the license and copyright notice
- 📋 State significant changes made to the original code
- 📋 Include the original attribution

For more details, see the LICENSE file in this repository or visit [opensource.org/licenses/MIT](https://opensource.org/licenses/MIT).

---

## Version History

**v1.0** — Initial release with batch processing and API calling

---

**Made with ⚡ for API developers**
