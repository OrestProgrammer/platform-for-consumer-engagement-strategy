import React, { useEffect, useState } from 'react';
import { Link, useNavigate } from "react-router-dom";

const Home = () => {

    const navigate = useNavigate();
    const [loginUser, setLoginUser] = useState(null);
    const [language, setLanguage] = useState("python");
    const [open, setOpen] = useState({ all: false });
    const columnDescriptions = {
        'consumer_zip_code': { type: 'string', description: 'The postal zip code of the consumer.', example: '07087' },
        'consumer_country': { type: 'string', description: 'The country where the consumer resides.', example: 'Brazil' },
        'consumer_state': { type: 'string', description: 'The state where the consumer resides.', example: 'SP' },
        'consumer_city': { type: 'string', description: 'The city where the consumer resides.', example: 'Sao Paulo' },
        'consumer_age': { type: 'integer', description: 'The age of the consumer.', example: '25' },
        'geolocation_latitude': { type: 'float', description: 'The geolocation latitude of the consumer.', example: '-14.664432' },
        'geolocation_longitude': { type: 'float', description: 'The geolocation longitude of the consumer.', example: '-57.263536' },
        'consumer_age_label': { type: 'string', description: 'The age group label of the consumer.', example: 'YC' },
        'payment_type_set': { type: 'array', description: 'The set of payment types used by the consumer.', example: '["credit_card", "voucher"]' },
        'product_category_name_english_set': { type: 'array', description: 'The set of product categories purchased by the consumer.', example: '["telephony", "toys"]' },
        'order_quality_label_set': { type: 'array', description: 'The set of order quality labels for the consumer.', example: '["CO", "GO"]' },
        'item_price_category_label_set': { type: 'array', description: 'The label set for the price category of items purchased by the consumer.', example: '["CI", "MI", "EI"]' },
        'payment_label_set': { type: 'array', description: 'The payment label set for the consumer.', example: '["FP", "PP"]' },
        'consumer_amount_of_orders': { type: 'integer', description: 'The total number of orders placed by the consumer.', example: '11' },
        'consumer_amount_of_products': { type: 'integer', description: 'The total number of products purchased by the consumer.', example: '30' },
        'max_product_price': { type: 'float', description: 'The maximum price of the products purchased by the consumer.', example: '225.1' },
        'min_product_price': { type: 'float', description: 'The minimum price of the products purchased by the consumer.', example: '8.79' },
        'avg_product_price': { type: 'float', description: 'The average price of the products purchased by the consumer.', example: '33.21' },
        'total_delivery_price': { type: 'float', description: 'The total delivery price paid by the consumer.', example: '298.12' },
        'consumer_max_payments_amount': { type: 'float', description: 'The maximum payment amount by the consumer.', example: '108.84' },
        'consumer_min_payments_amount': { type: 'float', description: 'The minimum payment amount by the consumer.', example: '20.07' },
        'consumer_avg_payments_amount': { type: 'float', description: 'The average payment amount by the consumer.', example: '31.21' },
        'consumer_total_payments_amount': { type: 'float', description: 'The total payment amount by the consumer.', example: '1300.91' },
        'consumer_payment_types_count': { type: 'integer', description: 'The number of payment types used by the consumer.', example: '3' },
        'consumer_amount_of_reviews': { type: 'integer', description: 'The total number of reviews given by the consumer.', example: '7' },
        'consumer_amount_of_feedbacks': { type: 'integer', description: 'The total number of feedbacks given by the consumer.', example: '5' },
        'consumer_amount_of_good_orders': { type: 'integer', description: 'The number of orders marked as good by the consumer.', example: '10' },
        'consumer_amount_of_canceled_orders': { type: 'integer', description: 'The number of orders canceled by the consumer.', example: '2' },
        'consumer_amount_of_chip_items': { type: 'integer', description: 'The number of cheap items purchased by the consumer.', example: '4' },
        'consumer_amount_of_medium_items': { type: 'integer', description: 'The number of medium priced items purchased by the consumer.', example: '5' },
        'consumer_amount_of_expensive_items': { type: 'integer', description: 'The number of expensive items purchased by the consumer.', example: '3' },
    };

    useEffect(() => {
        const loggedUserJSON = localStorage.getItem('loggeduser');
        if (loggedUserJSON) {
            const loggedUser = JSON.parse(loggedUserJSON);
            setLoginUser(loggedUser);
        }
    }, [])

    const handleClick = (event) => {
        setLanguage(event.target.id);
    }


    const handleOpen = (column) => {
        setOpen({ ...open, [column]: !open[column] });
    }


    const PythonExample = (
        <pre class="shadow">
            <code>

                <pre>
                    <code>
                        {`
                from pyspark.sql import SparkSession
                import requests
                import json

                # Initialize Spark Session
                spark = SparkSession.builder \\
                            .appName('test_classification_api') \\
                            .getOrCreate()

                # Read data from CSV file
                input_df = spark.read.csv("path/to/your.csv", header=True, inferSchema=True)

                # Convert Spark DataFrame to pandas DataFrame
                df = input_df.toPandas()

                # Convert pandas DataFrame to list of dictionaries
                records = df.to_dict('records')

                # POST request to classify records
                url = 'http://127.0.0.1:5000/api/v1/global/classification'
                headers = {'Content-Type': 'application/json'}
                data = {
                    'PersonalAPIKey': 'Your_Personal_API_Key',
                    'Records': records
                }
                response = requests.post(url, headers=headers, data=json.dumps(data))

                # parse response
                if response.status_code == 200:
                    resp = response.json()
                    classified_data = resp['data']

                    # convert list of dictionaries to Spark DataFrame
                    classified_df = spark.createDataFrame(classified_data)
                    classified_df.show(100, False)
                else:
                    print(f"Request failed with status code {response.status_code}. {response.json()['error']}")
                `}
                    </code>
                </pre>

            </code>
        </pre>
    );

    return (
        <div class="container">
            <h2>Description</h2>

            <div class="card shadow mb-5 p-5">
                <p>This platform uses a customer classification system developed based on detailed data analysis and ML algorithms. The platform is focused on providing a comprehensive yet simple classification of your customer base, thereby improving your business understanding and decision-making processes.</p>
                <p>The platform takes into account key customer characteristics and divides them into one of three categories: best, normal, and occasional. This allows companies to improve their strategic planning and operational mechanisms by understanding the unique behaviour of every customer.</p>
                <p>"Best" are the best customers who have a lot of frequent purchases, have high receipts and hardly ever return purchased items. They are the most valuable to the company, as they provide the biggest amount of sales and make very good profit.</p>
                <p>"Normal" are potentially profitable customers. This group includes users who can bring a lot of value in the future. They do not make a lot of purchases or spend large amount of money now, but they are promising in the context of attraction to the "Best" category.</p>
                <p>"Occasional" are customers who rarely make purchases, usually make small purchases, or have a large number of returns. Also, they can have one-time purchases or can be random visitors. Identifying the characteristics that drive these "occasional" customers to become "normal" or "best" customers can dramatically increase your growth rates.</p>
                <p>"Nonapplicable" if the customer does not have a purchases in that category.</p>
                <p> So, this classification platform allows companies to get a better and deeper understanding of their customers, which can lead to creating more personalized customer engagement strategies ultimately increasing sales and customer satisfaction. </p>
            </div>
            {!loginUser &&
                <>
                    <h2>Sign In</h2>
                    <div class="card shadow mb-5 p-5">
                        <p>To get started, please sign in.</p>
                        <button class="btn btn_login btn-primary" onClick={() => navigate('/login')}>
                            Sign In
                        </button>
                    </div>
                </>
            }
            <h2>Usage</h2>


            <div class="card shadow mb-5 p-5">
                <h3>1. Through the Platform</h3>
                <p>Navigate to the Classification tab and upload a CSV file with the customer data you want to analyze.</p>
                <h3>2. API Integration</h3>
                <p>The API can also be directly integrated into your codebase. The endpoint for classification is: <code>http://127.0.0.1:5000/api/v1/global/classification</code>.</p>
                <p>A request must include an array of 'Records' for classification and your 'PersonalAPIKey'.</p>
                <h3>Parameters</h3>
                <ul>
                    <li>Records - Array of records you want to classify.</li>
                    <li>PersonalAPIKey - Your key for using the API.</li>
                </ul>
            </div>

            <div class="card shadow mb-5 p-5">
                <h4>Description of fields in Records</h4>
                <button class="btn btn-primary mt-3 mb-3" type="button" onClick={() => setOpen(prevOpen => ({ ...prevOpen, all: !prevOpen.all }))}>
                    {open.all ? 'Hide Columns' : 'Show Columns'}
                </button>
                {open.all && (
                    <div className="classification-list">
                        <div className='card'>
                            {Object.keys(columnDescriptions).map((column, idx) => (
                                <div key={idx}>
                                    <div className="card-header">
                                        <h5 onClick={() => handleOpen(column)} className="mb-0">
                                            {column}
                                            <span >
                                                {open[column] ? '▲' : '▼'}
                                            </span>
                                        </h5>
                                    </div>

                                    <div className="card-body">
                                        {open[column] && (
                                            <p>
                                                Type: {columnDescriptions[column].type}<br />
                                                Description: {columnDescriptions[column].description}<br />
                                                Example: {columnDescriptions[column].example}
                                            </p>
                                        )}
                                    </div>
                                </div>
                            ))}
                        </div>
                    </div>
                )}
                <h4 className='pt-4'>API Request Example:</h4>
                <pre class="shadow">
                    <code>
                        {`POST http://127.0.0.1:5000/api/v1/global/classification
Content-Type: application/json
Body:
{
  "PersonalAPIKey": "your_personal_api_key",
  "Records": [
    {
      "consumer_zip_code": "your_zip_code",
      "consumer_country": "your_country",
      "consumer_state": "your_state",
      "consumer_city": "your_city",
      "consumer_age": "your_age",
      "geolocation_latitude": "your_latitude",
      "geolocation_longitude": "your_longitude",
      "consumer_age_label": "your_age_label",
      "payment_type_set": ["your_payment_type1", "your_payment_type2"],
      "product_category_name_english_set": ["your_category1", "your_category2"],
      "order_quality_label_set": ["your_quality_label"],
      "item_price_category_label_set": ["your_price_category_label"],
      "payment_label_set": ["your_payment_label"],
      "consumer_amount_of_orders": "your_amount_of_orders",
      "consumer_amount_of_products": "your_amount_of_products",
      "max_product_price": "your_max_product_price",
      "min_product_price": "your_min_product_price",
      "avg_product_price": "your_avg_product_price",
      "avg_delivery_price": "your_avg_delivery_price",
      "total_delivery_price": "your_total_delivery_price",
      "consumer_max_payments_amount": "your_max_payments_amount",
      "consumer_min_payments_amount": "your_min_payments_amount",
      "consumer_avg_payments_amount": "your_avg_payments_amount",
      "consumer_total_payments_amount": "your_total_payments_amount",
      "consumer_payment_types_count": "your_payments_types_count",
      "consumer_amount_of_reviews": "your_amount_of_reviews",
      "consumer_amount_of_feedbacks": "your_amount_of_feedbacks",
      "consumer_amount_of_good_orders": "your_amount_of_good_orders",
      "consumer_amount_of_canceled_orders": "your_amount_of_canceled_orders",
      "consumer_amount_of_chip_items": "your_amount_of_chip_items",
      "consumer_amount_of_medium_items": "your_amount_of_medium_items",
      "consumer_amount_of_expensive_items": "your_amount_of_expensive_items"
    }
  ]
}`}
                    </code>
                </pre>

            </div>
            <div class="card shadow mb-5 p-5">
                <h4>API Response Codes</h4>
                <table class="table table-code" style={{ marginTop: '20px', marginBottom: '50px' }}>
                    <thead>
                        <tr>
                            <th>Code</th>
                            <th>Description</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>200</td>
                            <td>Operation was successful. The classified records are returned.</td>
                        </tr>
                        <tr>
                            <td>400</td>
                            <td>Bad request. The request must contain "Records" and "PersonalAPIKey".</td>
                        </tr>
                        <tr>
                            <td>402</td>
                            <td>Payment required. The uploaded file exceeded quota limits.</td>
                        </tr>
                        <tr>
                            <td>404</td>
                            <td>The provided "PersonalAPIKey" is not valid.</td>
                        </tr>
                        <tr>
                            <td>500</td>
                            <td>Server error. Please try again later.</td>
                        </tr>
                    </tbody>
                </table>

                <div>
                    <h4>Example of API usage in the code</h4>
                    <div>
                        <ul class="nav nav-tabs">
                            <li class="nav-item">
                                <button class="nav-link active" onClick={handleClick}>PySpark</button>
                            </li>
                        </ul>

                    </div>
                    {language === 'python' && PythonExample}
                </div>
            </div>
        </div>
    );
}

export default Home;