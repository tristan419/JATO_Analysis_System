这份操作指南是为你量身定制的“省钱版” **JATO Analysis System** 部署方案。它结合了俄勒冈（Oregon）极低的 Spot 价格和自动化运维，将你的月成本压缩至 $20 左右。

---

# 🚀 JATO Dashboard 部署实战清单 (省钱稳健版)

## 第一阶段：AWS 基础设施准备

* [ ] **登录 AWS 俄勒冈区域** ：确保控制台右上角显示为 `Oregon (us-west-2)`。
* [ ] **创建 Key Pair (密钥对)** ：下载并保存好 `.pem` 文件。
* [ ] **申请 Elastic IP (弹性 IP)** ：
  * 在 EC2 菜单找到 `Elastic IPs` -> `Allocate Elastic IP`。
  * **原因** ：定时开关机会导致普通公网 IP 变动，绑定弹性 IP 后，你的访问地址将永远固定。
* [ ] **配置安全组 (Security Group)** ：
  * 入站规则 1：`SSH (22)` -> 来源设为 `My IP`。
  * 入站规则 2：`HTTP (80)` -> 来源设为 `0.0.0.0/0`（生产环境）。
  * 入站规则 3：`HTTPS (443)` -> 来源设为 `0.0.0.0/0`（如果后续加域名）。

## 第二阶段：启动竞价实例 (Spot Instance)

* [ ] **发起 Spot 请求** ：
  * 选择机型：`t3.xlarge` (4 vCPU / 16GB RAM)。
  * **关键勾选** ：在 `Advanced details` -> `Purchasing option` 勾选 `Request Spot Instances`。
  * **持久化设置** ：`Interruption behavior` 选 `Stop`（而不是 Terminate），这样被收回时只是关机，数据还在。
* [ ] **绑定弹性 IP** ：将第一阶段申请的 IP 关联到这台新实例。
* [ ] **运行初始化脚本** ：
  * SSH 进入服务器，执行你文档里的：
    **Bash**

    ```
    sudo bash 03_Scripts/deploy/aws/bootstrap_ubuntu.sh /opt/JATO_Analysis_System
    ```

## 第三阶段：定时开关机自动化 (Lambda + EventBridge)

* [ ] **创建 IAM 角色** ：创建一个名为 `EC2-Scheduler-Role` 的角色，赋予 `EC2StartStopAccess` 权限。
* [ ] **编写 Lambda 函数 (Python)** ：
  * 创建函数 `JATO-Scheduler`。
  * 示例代码（自动识别带有 `Project: JATO` 标签的实例并开关）：
    **Python**

    ```
    import boto3
    ec2 = boto3.client('ec2', region_name='us-west-2')
    def lambda_handler(event, context):
        action = event.get('action')
        instances = ['你的实例ID']
        if action == 'start':
            ec2.start_instances(InstanceIds=instances)
        elif action == 'stop':
            ec2.stop_instances(InstanceIds=instances)
    ```
* [ ] **配置 EventBridge 触发器** ：
  * 规则 1：`JATO-Morning-Start` -> Cron 表达式 `0 1 * * ? *` (北京时间早上 9 点) -> 输入 JSON `{"action": "start"}`。
  * 规则 2：`JATO-Night-Stop` -> Cron 表达式 `0 13 * * ? *` (北京时间晚上 9 点) -> 输入 JSON `{"action": "stop"}`。

## 第四阶段：上线验证

* [ ] **运行压测** ：使用 `k6` 执行 20 并发测试，确保 P95 响应在 8s 内。
* [ ] **检查 Swap** ：确保脚本创建了 4GB 的 Swap 分区，防止瞬间并发拉爆内存。
* [ ] **配置 GitHub Actions** ：将 `EC2_HOST` 更新为新的弹性 IP。

---

# 💡 还需要准备什么？

为了让这个方案达到“生产级”的稳定，你还需要考虑以下三个“补丁”：

### 1. 域名与简单的 SSL (推荐)

* **需求** ：你的 20 个用户如果直接输入 IP 访问，会显得不够专业，且浏览器会报“不安全”。
* **操作** ：买一个最便宜的 `.com` 或 `.net` 域名（约 $10/年），解析到你的弹性 IP，并用 `Certbot` 申请一个免费的 Let's Encrypt 证书。

### 2. 数据热备份 (S3)

* **需求** ：虽然 Spot 设为 Stop 模式很安全，但如果整个实例被 AWS 物理回收（极少见），或者磁盘坏了，你的 Parquet 数据就丢了。
* **操作** ：在你的 `elt_worker.py` 脚本末尾加一行代码，将生成的 `jato_full_archive.parquet` 自动同步一份到  **Amazon S3** 。S3 的存储费用几乎可以忽略不计。

### 3. 监控报警 (CloudWatch)

* **需求** ：你可能想知道服务器是否真的在 21:00 关机了，或者内存是否快满了。
* **操作** ：在 CloudWatch 设置一个  **Billing Alarm** （账单告警），一旦本月预估消费超过 $35，立马发邮件给你。这样可以防止任何意外导致的费用暴增。

---

**下一步建议：**

如果你准备好了，我可以先为你提供那个具体的 **Lambda 自动开关机 Python 脚本** 和  **Cron 时间换算表** （AWS 用的是 UTC 时间，和北京时间有 8 小时时差，很容易设错）。你需要这个脚本吗？
