# 代码审核规则集

## 资源释放
- **编程语言**：Go
- **问题描述**：未正确关闭资源（如文件、网络连接、数据库连接等）会导致资源泄漏，长时间运行的程序可能会耗尽系统资源
- **问题分类**：资源泄漏
- **问题等级**：Critical issues（必须修复，影响功能/安全）
- **代码正例**：file, err := os.Open("file.txt")
if err != nil {
    return err
}
defer file.Close()
// 处理文件内容    ---conn, err := net.Dial("tcp", "example.com:80")
if err != nil {
    return err
}
defer conn.Close()
// 使用连接- **代码反例**：file, err := os.Open("file.txt")
if err != nil {
    return err
}
// 没有关闭文件，可能导致资源泄漏
// 处理文件内容    ---func processData() error {
    db, err := sql.Open("mysql", "user:password@/dbname")
    if err != nil {
        return err
    }
    // 缺少 defer db.Close()
    rows, err := db.Query("SELECT * FROM table")
    // 处理查询结果
    return nil
}
## 错误处理
- **编程语言**：Go
- **问题描述**：忽略错误返回值会导致程序在出现问题时继续执行，可能引发更严重的错误或数据损坏
- **问题分类**：错误处理
- **问题等级**：Critical issues（必须修复，影响功能/安全）
- **代码正例**：data, err := ioutil.ReadFile("config.json")
if err != nil {
    log.Fatalf("无法读取配置文件: %v", err)
}
var config Config
if err := json.Unmarshal(data, &config); err != nil {
    log.Fatalf("解析配置失败: %v", err)
}    ---result, err := db.Exec("UPDATE users SET status = ? WHERE id = ?", "active", userID)
if err != nil {
    return fmt.Errorf("更新用户状态失败: %w", err)
}
affected, err := result.RowsAffected()
if err != nil {
    return fmt.Errorf("获取影响行数失败: %w", err)
}
if affected == 0 {
    return fmt.Errorf("未找到ID为%d的用户", userID)
}- **代码反例**：data, _ := ioutil.ReadFile("config.json") // 忽略错误
var config Config
json.Unmarshal(data, &config) // 忽略错误    ---db.Exec("UPDATE users SET status = ? WHERE id = ?", "active", userID)
// 没有检查错误或影响的行数
## 并发安全
- **编程语言**：Go
- **问题描述**：在并发环境中未使用适当的同步机制访问共享资源，可能导致数据竞争和不确定的行为
- **问题分类**：安全问题
- **问题等级**：Critical issues（必须修复，影响功能/安全）
- **代码正例**：type Counter struct {
    mu    sync.Mutex
    count int
}

func (c *Counter) Increment() {
    c.mu.Lock()
    defer c.mu.Unlock()
    c.count++
}

func (c *Counter) Value() int {
    c.mu.Lock()
    defer c.mu.Unlock()
    return c.count
}    ---var (
    cache     = make(map[string]string)
    cacheLock = sync.RWMutex{}
)

func Get(key string) (string, bool) {
    cacheLock.RLock()
    defer cacheLock.RUnlock()
    val, ok := cache[key]
    return val, ok
}

func Set(key, value string) {
    cacheLock.Lock()
    defer cacheLock.Unlock()
    cache[key] = value
}- **代码反例**：type Counter struct {
    count int
}

func (c *Counter) Increment() {
    c.count++ // 在并发环境中不安全
}

func (c *Counter) Value() int {
    return c.count // 在并发环境中可能返回不一致的值
}    ---var cache = make(map[string]string)

func Get(key string) (string, bool) {
    val, ok := cache[key] // 并发读取不安全
    return val, ok
}

func Set(key, value string) {
    cache[key] = value // 并发写入不安全
}
## SQL注入防护
- **编程语言**：Java
- **问题描述**：直接拼接SQL语句容易导致SQL注入攻击，可能使攻击者执行未授权的数据库操作
- **问题分类**：安全问题
- **问题等级**：Critical issues（必须修复，影响功能/安全）
- **代码正例**：String sql = "SELECT * FROM users WHERE username = ? AND password = ?";
PreparedStatement stmt = connection.prepareStatement(sql);
stmt.setString(1, username);
stmt.setString(2, password);
ResultSet rs = stmt.executeQuery();    ---public User findByEmail(String email) {
    String sql = "SELECT id, name, email FROM users WHERE email = ?";
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
        stmt.setString(1, email);
        try (ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return new User(
                    rs.getLong("id"),
                    rs.getString("name"),
                    rs.getString("email")
                );
            }
            return null;
        }
    }
}- **代码反例**：String sql = "SELECT * FROM users WHERE username = '" + username + "' AND password = '" + password + "'";
Statement stmt = connection.createStatement();
ResultSet rs = stmt.executeQuery(sql);    ---public boolean authenticateUser(String username, String password) {
    // 危险：直接拼接用户输入到SQL语句中
    String sql = "SELECT count(*) FROM users WHERE username = '" + username + 
                 "' AND password = '" + password + "'";
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery(sql);
    return rs.next() && rs.getInt(1) > 0;
}
## 空指针检查
- **编程语言**：Java
- **问题描述**：在访问对象或调用方法前未检查空引用，可能导致NullPointerException异常
- **问题分类**：边界条件
- **问题等级**：Critical issues（必须修复，影响功能/安全）
- **代码正例**：public String getUserName(User user) {
    if (user == null) {
        return "Guest";
    }
    return user.getName();
}    ---public void processOrder(Order order) {
    if (order == null) {
        throw new IllegalArgumentException("Order cannot be null");
    }
    
    List<Item> items = order.getItems();
    if (items != null && !items.isEmpty()) {
        for (Item item : items) {
            // 处理每个商品
        }
    }
}- **代码反例**：public String getUserName(User user) {
    return user.getName(); // 如果user为null，将抛出NullPointerException
}    ---public void processOrder(Order order) {
    List<Item> items = order.getItems(); // order可能为null
    for (Item item : items) { // items可能为null
        // 处理每个商品
    }
}
## 内存泄漏
- **编程语言**：JavaScript
- **问题描述**：在闭包中引用大型对象或DOM元素但未正确清理，导致内存无法被垃圾回收
- **问题分类**：资源泄漏
- **问题等级**：Needs to improve（需要优化，不影响核心功能）
- **代码正例**：function setupEventHandlers() {
  const button = document.getElementById('myButton');
  
  // 使用弱引用存储DOM元素
  let cleanup = () => {
    button.removeEventListener('click', handleClick);
    button = null; // 允许垃圾回收
  };
  
  function handleClick() {
    // 处理点击事件
  }
  
  button.addEventListener('click', handleClick);
  
  return cleanup; // 返回清理函数
}

// 使用
const cleanup = setupEventHandlers();
// 当不再需要时
cleanup();    ---class ResourceManager {
  constructor() {
    this.resources = new Map();

  }
  
  addResource(id, resource) {
    this.resources.set(id, resource);
  }
  
  getResource(id) {
    return this.resources.get(id);
  }
  
  releaseResource(id) {
    const resource = this.resources.get(id);
    if (resource && typeof resource.dispose === 'function') {
      resource.dispose();
    }
    this.resources.delete(id);
  }
  
  releaseAll() {
    for (const [id, resource] of this.resources.entries()) {
      if (typeof resource.dispose === 'function') {
        resource.dispose();
      }
    }
    this.resources.clear();
  }
}- **代码反例**：function createLargeDataProcessor() {
  // 大型数据对象
  const largeData = loadLargeDataSet();
  
  // 返回的函数持有对largeData的引用
  return function process() {
    // 即使不再需要largeData，它也不会被垃圾回收
    return largeData.filter(item => item.value > 100);
  };
}

// 创建处理器
const processor = createLargeDataProcessor();
// 使用处理器
processor();
// 没有方法释放largeData    ---function setupObserver() {
  const element = document.getElementById('observed');
  const observer = new MutationObserver(() => {
    console.log('Element changed:', element.innerHTML);
  });
  
  observer.observe(element, { childList: true, subtree: true });
  
  // 没有提供方法来断开观察器，导致内存泄漏
}
## 异步错误处理
- **编程语言**：JavaScript
- **问题描述**：未正确处理Promise中的错误，导致未捕获的异常和静默失败
- **问题分类**：错误处理
- **问题等级**：Critical issues（必须修复，影响功能/安全）
- **代码正例**：async function fetchUserData(userId) {
  try {
    const response = await fetch(`/api/users/${userId}`);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    const data = await response.json();
    return data;
  } catch (error) {
    console.error('Failed to fetch user data:', error);
    throw error; // 重新抛出以便调用者处理
  }
}

// 使用
fetchUserData(123)
  .then(user => {
    // 处理用户数据
  })
  .catch(error => {
    // 处理错误
  });- **代码反例**：async function fetchUserData(userId) {
  const response = await fetch(`/api/users/${userId}`);
  const data = await response.json(); // 未处理HTTP错误和解析错误
  return data;
}

// 使用时未捕获错误
fetchUserData(123)
  .then(user => {
    // 处理用户数据
  });
## 可变默认参数陷阱
- **编程语言**：Python
- **问题描述**：使用可变对象（如列表、字典）作为函数默认参数时，默认参数会在函数定义时初始化一次，后续调用会复用该对象，导致意外的状态累积
- **问题分类**：代码逻辑问题
- **问题等级**：Needs to improve（需要优化，不影响核心功能）
- **代码正例**：def add_item(item, items=None):
    # 使用不可变默认值，每次调用重新初始化
    if items is None:
        items = []
    items.append(item)
    return items

# 多次调用相互独立
print(add_item(1))  # [1]
print(add_item(2))  # [2]- **代码反例**：def add_item(item, items=[]):  # 可变对象作为默认参数
    items.append(item)
    return items

# 多次调用共享同一个列表，导致状态累积
print(add_item(1))  # [1]
print(add_item(2))  # [1, 2]（预期应为[2]）
## 线程池滥用
- **编程语言**：Java
- **问题描述**：频繁创建新线程池而不复用，会导致系统资源（线程、内存）耗尽，尤其是在高并发场景下
- **问题分类**：资源泄漏
- **问题等级**：Critical issues（必须修复，影响功能/安全）
- **代码正例**：// 复用单例线程池，避免重复创建
public class ThreadPoolManager {
    private static final ExecutorService executor = Executors.newFixedThreadPool(10);
    
    public static ExecutorService getExecutor() {
        return executor;
    }
    
    // 程序退出时关闭线程池
    public static void shutdown() {
        executor.shutdown();
    }
}

// 使用方式
ThreadPoolManager.getExecutor().submit(() -> {
    // 执行任务
});- **代码反例**：public void processTask(Runnable task) {
    // 每次调用创建新线程池，导致资源耗尽
    ExecutorService executor = Executors.newFixedThreadPool(10);
    executor.submit(task);
    // 未关闭线程池，进一步加剧资源泄漏
}
## var关键字作用域问题
- **编程语言**：JavaScript
- **问题描述**：使用`var`声明变量会导致变量提升和函数级作用域，可能引发变量覆盖、作用域污染等逻辑错误
- **问题分类**：代码逻辑问题
- **问题等级**：Needs to improve（需要优化，不影响核心功能）
- **代码正例**：function countItems() {
    // 使用let/const的块级作用域
    let count = 0;
    if (true) {
        let count = 1;  // 块内独立变量，不影响外部
        console.log('内部:', count);  // 1
    }
    console.log('外部:', count);  // 0（符合预期）
    return count;
}- **代码反例**：function countItems() {
    var count = 0;
    if (true) {
        var count = 1;  // var声明会提升到函数级，覆盖外部变量
        console.log('内部:', count);  // 1
    }
    console.log('外部:', count);  // 1（预期应为0，逻辑错误）
    return count;
}
## 数组越界访问
- **编程语言**：C
- **问题描述**：访问数组时未检查索引范围，可能导致读取/写入非法内存区域，引发程序崩溃或安全漏洞
- **问题分类**：边界条件
- **问题等级**：Critical issues（必须修复，影响功能/安全）
- **代码正例**：#include <stdio.h>

#define ARRAY_SIZE 5

int getElement(int arr[], int index) {
    // 检查索引是否在有效范围内
    if (index < 0 || index >= ARRAY_SIZE) {
        printf("Error: 索引越界\n");
        return -1;  // 返回错误标识
    }
    return arr[index];
}

int main() {
    int arr[ARRAY_SIZE] = {1, 2, 3, 4, 5};
    printf("%d\n", getElement(arr, 2));  // 3（正常访问）
    printf("%d\n", getElement(arr, 10)); // 错误提示（越界保护）
    return 0;
}- **代码反例**：#include <stdio.h>

#define ARRAY_SIZE 5

int getElement(int arr[], int index) {
    // 未检查索引范围，存在越界风险
    return arr[index];
}

int main() {
    int arr[ARRAY_SIZE] = {1, 2, 3, 4, 5};
    printf("%d\n", getElement(arr, 2));  // 3（正常）
    printf("%d\n", getElement(arr, 10)); // 访问非法内存，可能崩溃或返回随机值
    return 0;
}
## 智能指针循环引用
- **编程语言**：C++
- **问题描述**：`std::shared_ptr`相互引用形成循环时，引用计数无法归零，导致内存泄漏
- **问题分类**：资源泄漏
- **问题等级**：Critical issues（必须修复，影响功能/安全）
- **代码正例**：#include <memory>

class B;  // 前向声明

class A {
public:
    std::weak_ptr<B> b_ptr;  // 使用weak_ptr打破循环引用
    ~A() { printf("A被销毁\n"); }
};

class B {
public:
    std::shared_ptr<A> a_ptr;
    ~B() { printf("B被销毁\n"); }
};

int main() {
    auto a = std::make_shared<A>();
    auto b = std::make_shared<B>();
    a->b_ptr = b;  // weak_ptr不增加引用计数
    b->a_ptr = a;
    // 离开作用域时，引用计数归0，对象被正确销毁
    return 0;
}- **代码反例**：#include <memory>

class B;  // 前向声明

class A {
public:
    std::shared_ptr<B> b_ptr;  // shared_ptr形成循环
    ~A() { printf("A被销毁\n"); }
};

class B {
public:
    std::shared_ptr<A> a_ptr;  // shared_ptr形成循环
    ~B() { printf("B被销毁\n"); }
};

int main() {
    auto a = std::make_shared<A>();
    auto b = std::make_shared<B>();
    a->b_ptr = b;  // 引用计数+1
    b->a_ptr = a;  // 引用计数+1
    // 离开作用域时，引用计数仍为1，对象未销毁（内存泄漏）
    return 0;
}
## 硬编码敏感信息
- **编程语言**：All
- **问题描述**：直接在代码中硬编码密码、密钥、API地址等敏感信息，可能导致信息泄露（如代码仓库暴露）、环境切换困难
- **问题分类**：硬编码问题
- **问题等级**：Critical issues（必须修复，影响功能/安全）
- **代码正例**：# Python示例：从环境变量读取配置
import os
import json

# 从环境变量获取密钥
api_key = os.getenv("API_SECRET_KEY")
# 从配置文件读取地址（非代码库托管）
with open("config.json") as f:
    config = json.load(f)
api_url = config["api_url"]```java
// Java示例：从配置文件读取
import java.io.FileInputStream;
import java.util.Properties;

public class Config {
    private static final Properties props = new Properties();
    static {
        try {
            // 加载外部配置文件
            props.load(new FileInputStream("config.properties"));
        } catch (Exception e) {
            throw new RuntimeException("加载配置失败", e);
        }
    }
    public static String getDbPassword() {
        return props.getProperty("db.password");
    }
}- **代码反例**：# Python反例：硬编码密钥
api_key = "sk-1234567890abcdef"  # 敏感信息直接暴露
api_url = "https://prod-api.example.com"  # 环境切换需改代码```java
// Java反例：硬编码数据库密码
public class DbConfig {
    // 密码硬编码，代码提交后全网可见
    public static final String DB_PASSWORD = "root123456";
    public static final String DB_URL = "jdbc:mysql://prod-db:3306/db";
}
## 注释缺失或不准确
- **编程语言**：All
- **问题描述**：关键逻辑、复杂算法或特殊处理缺少注释，或注释与代码功能不一致，会导致维护困难、理解偏差
- **问题分类**：可读性问题
- **问题等级**：Nice to have（建议优化，仅提升体验）
- **代码正例**：// JavaScript示例：清晰注释
/**
 * 计算两个数的最大公约数（欧几里得算法）
 * @param {number} a - 正整数
 * @param {number} b - 正整数（a >= b）
 * @returns {number} 最大公约数
 */
function gcd(a, b) {
    // 递归终止条件：余数为0时，当前b为最大公约数
    if (b === 0) return a;
    // 递归：用b和a%b继续计算
    return gcd(b, a % b);
}```c++
// C++示例：注释与逻辑一致
#include <vector>
/**
 * 移除数组中重复元素（原地修改）
 * 注意：仅保留第一个出现的元素，返回新长度
 */
int removeDuplicates(std::vector<int>& nums) {
    if (nums.empty()) return 0;
    int i = 0;  // 慢指针：指向最后一个不重复元素
    // 快指针遍历数组，发现新元素则更新慢指针
    for (int j = 1; j < nums.size(); j++) {
        if (nums[j] != nums[i]) {
            i++;
            nums[i] = nums[j];
        }
    }
    return i + 1;
}- **代码反例**：// JavaScript反例：注释缺失+错误
// 计算结果（无具体说明）
function gcd(a, b) {
    if (b === 0) return a;
    return gcd(a, b % a);  // 代码错误（参数顺序反了），注释未提示
}```c++
// C++反例：注释与代码矛盾
// 保留所有重复元素，返回原长度（注释错误）
int removeDuplicates(std::vector<int>& nums) {
    if (nums.empty()) return 0;
    int i = 0;
    for (int j = 1; j < nums.size(); j++) {
        if (nums[j] != nums[i]) {
            i++;
            nums[i] = nums[j];
        }
    }
    return i + 1;  // 实际返回去重后长度
}
