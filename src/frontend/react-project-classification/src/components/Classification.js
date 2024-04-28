import React, { useState, useEffect } from 'react';
import Papa from 'papaparse';
import "./styles.css"
import { useNavigate } from "react-router-dom";


const Classification = () => {
    const loginUser = JSON.parse(localStorage.getItem('loggeduser'));
    const [user, setUser] = useState(loginUser);
    const [file, setFile] = useState(null);
    const [data, setData] = useState(null);
    const [previewData, setPreviewData] = useState([]);
    const [open, setOpen] = useState({ all: true });
    const [totalRecords, setTotalRecords] = useState(0);
    const [bestCount, setBestCount] = useState(0);
    const [normalCount, setNormalCount] = useState(0);
    const [badCount, setBadCount] = useState(0);
    const navigate = useNavigate();
    const [error, setError] = useState(null);


    useEffect(() => {
        if (loginUser) {
            const headers = new Headers();
            headers.set('Authorization', `Basic ${btoa(`${loginUser.username}:${loginUser.password}`)}`);
            headers.set('content-type', 'application/json');

            fetch(`http://127.0.0.1:5000/api/v1/user/${loginUser.username}`, {
                method: 'GET',
                headers,
            })
                .then((response) => {
                    if (response.status === 200) return response.json();
                })
                .then((data) => {
                    setUser(data.user)
                });
        } else {
            navigate('/login');
        }
    }, []);

    const handleFileChange = (e) => {
        console.log(e.target.files[0])
        setFile(e.target.files[0]);
    }

    const handleFormSubmit = (e) => {
        e.preventDefault();

        const formData = new FormData();
        formData.append('file', file);
        formData.append('username', loginUser.username);

        const headers = new Headers();
        headers.set('Authorization', `Basic ${btoa(`${loginUser.username}:${loginUser.password}`)}`);

        fetch('http://127.0.0.1:5000/api/v1/classification', {
            method: 'POST',
            body: formData,
            headers,
        })
            .then(response => response.json())
            .then(data => {
                if (data.status === 'success') {
                    setData(data.data);
                    const csvData = Papa.parse(data.data, { header: true });
                    setPreviewData(csvData.data.slice(0, 20));
                    calculateMetrics(csvData.data);
                } else {
                    setError(data.data)
                }
            }).catch((e) => {
                setError(e.message)
            });
    }
    const handleOpen = (column) => {
        setOpen(prevOpen => ({ ...prevOpen, [column]: !prevOpen[column] }));
    };

    const handleDownload = () => {
        const blob = new Blob([data], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement('a');
        const url = URL.createObjectURL(blob);
        link.href = url;
        link.setAttribute('download', 'processed_data.csv');
        document.body.appendChild(link);
        link.click();
    }

    const calculateMetrics = (data) => {
        let bestCount = 0;
        let normalCount = 0;
        let occasionalCount = 0;

        const categoryColumns = Object.keys(data[0]).filter(key => key.startsWith('category_'));

        data.forEach(record => {
            categoryColumns.forEach(column => {
                if(record[column] === "Best") {
                    bestCount++;
                } else if(record[column] === "Normal") {
                    normalCount++;
                } else if(record[column] === "Occasional") {
                    occasionalCount++;
                }
            });
        });

        setBestCount(bestCount);
        setNormalCount(normalCount);
        setBadCount(occasionalCount);
        setTotalRecords(bestCount + normalCount + occasionalCount);

    }

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

    if (!loginUser) {
        navigate("/login")
    } else {
        return (
            <div class='container'>
                <form onSubmit={handleFormSubmit}>
                    <h3>To use our customer classifier you have to upload a CSV file with the following columns:</h3>
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

                    <div class="row mt-5">
                        <div class="col-md-4 mx-auto">
                            <div class="card shadow p-4">
                                <h5 class="text-center mb-4 text-muted">
                                    You can use CSV file to upload
                                </h5>
                                <h6 class="text-center">{file && file.name}</h6>
                                <label for="fileUpload" class="file-upload btn btn-primary btn-block">Browse for file ...
                                    <input id="fileUpload" onChange={handleFileChange} type="file" />
                                </label>

                                {file && (<button class="btn mt-2 btn-process btn-block" type="submit">Process file</button>)}
                            </div>
                        </div>

                    </div>


                    {previewData.length > 0 && (<div class="card shadow mt-5 mb-5">
                        <div class="row m-3">
                            <div class="table-csv">

                                <div class="">
                                    <table class="table table-striped">
                                        <thead>
                                            <tr>
                                                {Object.keys(previewData[0]).map((header, index) => <th key={index}>{header}</th>)}
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {previewData.map((row, index) => (
                                                <tr key={index}>
                                                    {Object.values(row).map((cell, idx) => <td key={idx}>{cell}</td>)}
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>


                            </div>
                            <button class="btn btn-primary mt-3" type="button" onClick={handleDownload}>Download Processed File</button>

                        </div>
                    </div>)}


                    <div class="row mt-5">
                        <div class="">
                            {data && (
                                <div class="card shadow text-center p-4">
                                    <p>Number of total classified records: {totalRecords}</p>
                                    <p>Number of records classified as "Best": {bestCount}</p>
                                    <p>Number of records classified as "Normal": {normalCount}</p>
                                    <p>Number of records classified as "Occasional": {badCount}</p>
                                </div>
                            )}
                        </div>
                    </div>


                </form>
                <p className='error-message'>{error}</p>
            </div>
        );
    }
}

export default Classification;