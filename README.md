# 🚫 Real-Time Spam Detection System (Spark + Kafka + Scala)

A real-time spam detection pipeline built with **Apache Spark Structured Streaming**, **Kafka**, and **Scala**. This system ingests live comment data, identifies **spammers** based on posting behavior, and flags **suspicious posts** accordingly.

---

## 🧩 Problem Statement

Social media platforms are frequently targeted by bots and spam accounts. Detecting spam behavior in real-time is critical for platform health. This project simulates a real-world moderation system that identifies:

- **Spammers**: Users posting >10 times in under 1 minutes
- **Suspicious Posts**: Posts receiving high engagement from suspicious users

---

## ⚙️ Tech Stack

- **Apache Spark Structured Streaming**
- **Scala**
- **Apache Kafka**

---

## 🧪 Features

- ✅ Real-time comment ingestion via Kafka
- ✅ Detects spammers using sliding window logic
- ✅ Flags posts based on cumulative suspicious user interactions
- ✅ Structured output to Kafka for downstream moderation pipelines
- ✅ Graceful shutdown and checkpoint support
- ✅ Stateless & stateful streaming operations

---

## 🗃️ Data Schema

### 🧑 User
| Column        | Type    |
|---------------|---------|
| user_id       | String  |
| username      | String  |
| name          | String  |
| flaggedSpammer| Boolean |

### 💬 Comment
| Column     | Type     |
|------------|----------|
| user_id    | Int      |
| comment_id | Int      |
| post_id    | Int      |
| text       | String   |
| timestamp  | Long     |

