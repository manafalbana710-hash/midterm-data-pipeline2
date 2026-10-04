خط البيانات الهجين لمعالجة الطلبات وجودة البيانات

1. الملخص التنفيذي وفكرة المشروع

تم تطوير هذا المشروع كحل لمعالجة بيانات طلبات المتاجر الإلكترونية، مع تطبيق مفهوم ELT وتنظيف البيانات غير المنظمة وحفظ النتائج في MongoDB.

يعتمد المشروع على محركين لمعالجة البيانات حسب حجم الملف:

Python Batch للملفات الصغيرة التي يكون حجمها أقل من أو يساوي 200 MB.

PySpark للملفات الكبيرة التي يتجاوز حجمها 200 MB.

يتم اختيار المحرك تلقائياً من خلال src/file_router.py.

يتم تطبيق 9 قواعد لجودة البيانات وتنظيف القيم غير الصحيحة.

يتم الاحتفاظ بالسجلات التي تحتاج إلى تصحيح مع سجل تدقيق للتعديلات.

يتم عزل السجلات التي لا يمكن تصحيحها في orders_quarantine مع أسباب العزل.

يتم استخدام MongoDB لتخزين البيانات النهائية مع دعم Upsert وIdempotency.

2. المعمارية وتدفق البيانات

يتكون المشروع من المراحل التالية:

المرحلة الأولى: استلام الملف وتحديد المحرك

يتم استقبال ملف CSV من مجلد data/، ثم يقوم:

src/file_router.py

بحساب حجم الملف واختيار المحرك المناسب:

إذا كان الحجم <= 200 MB يتم اختيار python_batch.

إذا كان الحجم > 200 MB يتم اختيار pyspark.

المرحلة الثانية: التحميل باستخدام Python Batch

يستخدم:

src/batch_loader.py

لقراءة الملفات الصغيرة باستخدام csv.DictReader على دفعات، ثم تخزين السجلات في:

orders_raw

مع معلومات التتبع مثل:

id_run

file_source

number_row_source

engine_used

at_ingested

record_raw

المرحلة الثالثة: معالجة الملفات الكبيرة باستخدام PySpark

يستخدم:

src/elt_million_pipeline.py

لمعالجة ملف:

data/million_sample.csv

باستخدام PySpark وSpark Standalone.

يتم قراءة البيانات باستخدام DataFrame API ثم تطبيق قواعد جودة البيانات، وبعد ذلك يتم تقسيم السجلات إلى سجلات صالحة/مصححة وسجلات Quarantine.

المرحلة الرابعة: تنظيف البيانات

يتم تطبيق القواعد الموجودة في:

src/quality_rules.py

على بيانات الطلبات.

المرحلة الخامسة: التخزين النهائي

يتم تخزين البيانات في MongoDB ضمن:

orders_validated

orders_quarantine

كما توجد مجموعة البيانات الخام:

orders_raw

3. الملفات والبيانات المستخدمة

يحتوي مجلد data/ على ملفات البيانات التالية:

small_sample.csv

عينة صغيرة تستخدم لاختبار مسار Python Batch.

orders_huge_mixed_quality.csv

ملف بيانات كبير يحتوي على حالات جودة مختلفة، ويستخدم لاختبار معالجة البيانات الكبيرة.

million_sample.csv

ملف الاختبار الرئيسي الذي يحتوي على:

1,000,000 سجل

ويستخدم لإثبات قدرة PySpark على معالجة مليون سجل.

4. قواعد جودة البيانات

يتم تطبيق 9 قواعد رئيسية لتنظيف البيانات وتصحيحها:

#

قاعدة التنظيف

الوظيفة

1

Arabic Numbers

تحويل الأرقام العربية إلى أرقام قياسية

2

Currency Normalization

توحيد العملات وإزالة النصوص غير الضرورية

3

Thousands Separators

معالجة فواصل الآلاف والرموز الرقمية

4

Price in Words

التعامل مع الأسعار المكتوبة بالكلمات

5

Phone Normalization

توحيد أرقام الهواتف

6

Email Normalization

تنظيف وتوحيد البريد الإلكتروني

7

Date Normalization

توحيد صيغ التواريخ

8

Spaces and Synonyms

معالجة المسافات والمرادفات

9

Total Recalculation

إعادة حساب إجمالي الطلب

5. سجل التدقيق والتصحيح

عند تصحيح سجل، يتم الاحتفاظ بمعلومات التغيير داخل الحقل:

corrections

ويشمل سجل التدقيق:

اسم الحقل.

القيمة الأصلية.

القيمة بعد التصحيح.

رمز قاعدة التنظيف المستخدمة.

وبذلك يمكن معرفة التغييرات التي تمت على البيانات وعدم فقدان أثر عملية التنظيف.

6. Quarantine

السجلات التي تحتوي على أخطاء لا يمكن تصحيحها بأمان يتم عزلها في:

orders_quarantine

ومن أمثلة أسباب العزل:

MISSING_ORDER_ID

MISSING_CUSTOMER_ID

INVALID_IMPOSSIBLE_DATE

CORRUPTED_ITEMS_JSON

EMPTY_ITEMS

UNKNOWN_PRICE

AMBIGUOUS_NEGATIVE_VALUE

INVALID_PHONE

INVALID_EMAIL

INVALID_DELIVERY_COST

يتم الاحتفاظ بسبب العزل حتى يمكن مراجعة السجل لاحقاً.

7. MongoDB

يستخدم المشروع MongoDB على:

mongodb://localhost:27017

وقاعدة البيانات المستخدمة:

midterm_data_pipeline2

المجموعات:

orders_raw

orders_validated

orders_quarantine

يتم استخدام عمليات Upsert عند حفظ البيانات النهائية، مما يساعد على منع إنشاء سجلات مكررة عند إعادة معالجة نفس البيانات.

8. Idempotency و Upsert

يعتمد المشروع على مفتاح الطلب:

order_id

عند إعادة معالجة سجل موجود، يتم تحديث السجل بدلاً من إنشاء سجل جديد.

وقد تم اختبار إعادة معالجة سجل موجود مسبقاً، وكانت النتيجة:

لا يتم إنشاء سجل جديد.

لا يحدث تكرار في البيانات.

يتم الاحتفاظ بالسجل الموجود.

وهذا يثبت عمل آلية Idempotency في عملية Upsert.

9. Spark Standalone

يستخدم المشروع Spark Standalone لمعالجة ملف المليون.

إعداد الـMaster الحالي:

spark://172.22.144.1:7077

إعداد الـWorker:

عدد الأنوية: 8

الذاكرة: 30.9 GiB

إصدار Spark: 4.2.0

واجهات Spark:

Master UI: http://localhost:8080

Worker UI: http://localhost:8081

يجب تشغيل الـMaster والـWorker قبل تشغيل Pipeline الخاص بالمليون.

10. نتيجة معالجة المليون

تم تنفيذ اختبار ناجح لمعالجة ملف:

million_sample.csv

والذي يحتوي على:

1,000,000 سجل

النتائج الموثقة:

المقياس

النتيجة

السجلات الخام

1,000,000

السجلات المعالجة

1,000,000

السجلات المصححة

918,742

سجلات Quarantine

81,258

قواعد الجودة

9

زمن التنفيذ

215.946 ثانية

معدل المعالجة

4,630.78 سجل/ثانية

عدد أنوية Worker

8

ذاكرة Worker

30.9 GiB

فحص العدد

PASSED

فحص Upsert

PASSED

فحص Idempotency

PASSED

11. معادلة اتساق البيانات

يتم التحقق من اتساق عملية المعالجة باستخدام:

raw_count = valid_count + corrected_count + quarantine_count

وفي اختبار المليون:

1,000,000 = 0 + 918,742 + 81,258

حيث تمثل 918,742 السجلات التي انتهت في مسار السجلات الصالحة/المصححة، وتمثل 81,258 السجلات المعزولة.

وكان فحص عدد السجلات:

PASSED

12. النتائج والتقارير

يتم حفظ نتائج الاختبار في:

reports/results.json

كما يوجد تقرير نصي في:

reports/results.md

ويحتوي التقرير على أعداد السجلات، زمن التنفيذ، معدل المعالجة، إعدادات Spark، ونتائج اختبارات Upsert وIdempotency.

13. هيكل المشروع

midterm-data-pipeline2/
│
├── config/
│   └── settings.py
│
├── data/
│   ├── small_sample.csv
│   ├── orders_huge_mixed_quality.csv
│   └── million_sample.csv
│
├── src/
│   ├── main.py
│   ├── file_router.py
│   ├── batch_loader.py
│   ├── spark_loader.py
│   ├── elt_pipeline.py
│   ├── elt_million_pipeline.py
│   ├── quality_rules.py
│   ├── mongo_setup.py
│   ├── metrics.py
│   ├── create_small_sample.py
│   └── create_million_sample.py
│
├── tests/
│   ├── test_classification.py
│   └── test_cleaning_rules.py
│
├── reports/
│   ├── results.json
│   ├── results.md
│   └── screenshots/
│
├── requirements.txt
└── README.md

14. المتطلبات

المشروع يستخدم:

Python 3.11

PySpark 4.2.0

PyMongo

MongoDB

Spark Standalone

لتثبيت المكتبات:

pip install -r requirements.txt

15. التشغيل

تشغيل MongoDB

يجب التأكد من تشغيل خدمة MongoDB قبل تنفيذ الـPipeline.

تشغيل Spark Master

spark-class org.apache.spark.deploy.master.Master

يظهر عنوان الـMaster الحالي:

spark://172.22.144.1:7077

تشغيل Spark Worker

في نافذة PowerShell أخرى:

spark-class org.apache.spark.deploy.worker.Worker spark://172.22.144.1:7077

يجب التأكد من ظهور:

Successfully registered with master

تشغيل معالجة المليون

من VS Code يتم فتح:

src/elt_million_pipeline.py

ثم الضغط على:

Run

16. الاختبارات والتحقق

يشمل المشروع التحقق من:

اختيار المحرك حسب حجم الملف.

تطبيق قواعد تنظيف البيانات.

تصنيف السجلات.

عزل السجلات غير القابلة للتصحيح.

التحقق من عدد السجلات.

Upsert في MongoDB.

Idempotency عند إعادة معالجة السجلات.

معالجة ملف يحتوي على مليون سجل باستخدام PySpark.

17. الخلاصة

يوفر المشروع خط بيانات هجيناً لمعالجة ملفات الطلبات الصغيرة والكبيرة، مع اختيار المحرك المناسب حسب حجم الملف.

تم استخدام Python Batch للملفات الصغيرة وPySpark للملفات الكبيرة، مع تطبيق 9 قواعد لجودة البيانات، وتوفير سجل تدقيق للتصحيحات وعزل للسجلات غير القابلة للتصحيح.

كما تم استخدام MongoDB لتخزين البيانات، مع دعم Upsert وIdempotency، وتم تنفيذ اختبار فعلي على ملف يحتوي على مليون سجل بنتيجة معالجة ناجحة ومعدل معالجة موثق.

---

# 18. إضافات المشروع النهائي (Big Data - Phase 2)

تم استكمال وتوسيع المشروع ليشمل متطلبات المرحلة الثانية (المشروع النهائي) عبر توفير المكونات التالية:

### 18.1 واجهة الـ API الموحدة (FastAPI)
تم توفير واجهة برمجية موحدة للتشغيل والاختبار باستخدام FastAPI، وتتوفر صفحة توثيق Swagger عبر:
```bash
uvicorn src.api:app --reload --port 8000
```
أو عبر المتصفح: `http://localhost:8000/docs`

**الأطراف المتاحة (Endpoints):**
- `GET /health`: فحص حالة النظام وقاعدة البيانات.
- `POST /ingest`: تشغيل مسار الإدخال والتوجيه المعتمد بالمشروع النصفي.
- `POST /indexes`: إنشاء الفهارس الثلاثة المطلوب.
- `GET /queries`: عرض قائمة الاستعلامات المتاحة.
- `GET /queries/{name}`: تشغيل استعلام محدد أو تحليل `explain_analysis`.
- `GET /aggregations`: قائمة التقارير التجميعية الـ 5.
- `GET /aggregations/{name}`: تنفيذ التقرير التجميعي المطلوب.
- `POST /refresh-mv`: إجراء التحديث التزايدي للعروض المادية (Materialized Views).
- `GET /jobs`: عرض سجل المهام المجدولة المنجزة.
- `POST /jobs/{name}/run`: تشغيل مهمة مجدولة يدوياً.

### 18.2 الاستعلامات والفهارس والـ Explain
- **5 استعلامات عملية:**
  1. `customer_orders`: تاريخ طلبات العميل مرتبة حسب التاريخ.
  2. `city_status`: الفلترة حسب المدينة وحالة التوصيل.
  3. `date_range`: استعلام الطلبات بين تاريخين.
  4. `email_or_phone`: البحث ببيانات التواصل.
  5. `high_value_orders`: الطلبات ذات القيم المالية العالية.
- **3 الفهارس (Indexes):**
  1. `idx_customer_order_date` (**Compound Index**): `[("customer_id", 1), ("order_date", -1)]`
  2. `idx_status` (Single Index): `[("status", 1)]`
  3. `idx_city` (Single Index): `[("city", 1)]`
- **نتائج `explain("executionStats")`:**
  - الانتقال من المسح الشامل `COLLSCAN` و `SORT` بالذاكرة إلى التصفح المباشر بالفهرس `IXSCAN / FETCH`.
  - انخفاض زمن التنفيذ من **33ms** إلى **15ms** في استعلامات الطلبات المعقدة.

### 18.3 تقارير التجميعات (Aggregations)
1. `sales_by_city`: المبيعات وإجمالي الإيراد ومعدل الطلب حسب المدينة.
2. `top_products`: أفضل المنتجات مبيعاً من حيث الكمية والإيراد.
3. `top_customers`: كبار العملاء إنفاقاً وعدد الطلبات.
4. `sales_by_period`: إحصائيات المبيعات اليومية.
5. `orders_by_status`: توزيع الطلبات حسب الحالة التشغيلية.

### 18.4 العروض المادية والتحديث التزايدي (Materialized Views)
- **`daily_sales_summary`**: عرض مادي مجمّع للمبيعات اليومية.
- **`top_products_summary`**: عرض مادي مجمّع لأداء المنتجات.
- **آلية التحديث التزايدي (Incremental Refresh):** يعتمد على تخزين `watermark` لآخر طابع زمني للمعالجة (`last_at_ingested`) وتحديث أو إضافة السجلات الجدد فقط دون إعادة حساب المجموعة بالكامل.

### 18.5 المهام المجدولة (Scheduled Jobs)
- **`refresh_materialized_views_job`**: المهمة المجدولة لتحديث العروض المادية تزايدياً.
- **`periodic_system_audit_job`**: المهمة المجدولة لفحص صحة وسجلات النظام.
- **سجل التنفيذ (`job_logs`):** تسجيل وقت البداية، وقت النهاية، حالة النجاح/الفشل، والتفاصيل التشغيلية.

### 18.6 تشغيل كافة الاختبارات
لتشغيل جميع اختبارات المشروع النصفي والنهائي:
```bash
python -m pytest tests/
```

