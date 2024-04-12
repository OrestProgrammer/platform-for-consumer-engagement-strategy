import React, { useEffect, useState } from 'react';
import { useNavigate, useParams } from "react-router-dom";
import moment from "moment";


const ProcessingHistory = () => {

    const loginUser = JSON.parse(localStorage.getItem('loggeduser'));
    const [history, setHistory] = useState([]);
    const navigate = useNavigate();

    useEffect(() => {
        if (loginUser) {

            const headers = new Headers();
            headers.set('Authorization', 'Basic ' + btoa(loginUser.username + ":" + loginUser.password));
            headers.set('content-type', 'application/json');

            const input = {
                username: loginUser.username
            };

            fetch(`http://127.0.0.1:5000/api/v1/processinghistory`, {
                method: 'POST',
                body: JSON.stringify(input),
                headers
            }).then((response) => {
                if (response.status === 200) {
                    return response.json();
                }
            }).then((data) => {
                setHistory(data.history)
            });
        } else {
            navigate('/login');
        }
    }, []);

    if (!loginUser) {
        navigate("/login")
    } else {
        return (
            <div class="container">
                <h2>History of data classification:</h2>

                <div class="card shadow mb-5 p-5">
                    {history.length === 0 && <div>
                        <p>The history of your processing is empty!</p>
                    </div>}

                    <table class="table">
                        <thead>
                            <tr>
                                <th>Username</th>
                                <th>Email</th>
                                <th>Token</th>
                                <th>Amount</th>
                                <th>Timestamp</th>
                                <th>Status</th>
                            </tr>
                        </thead>
                        <tbody>
                            {history.length !== 0 && Object.values(history).map(e => (
                                <tr key={e.id}>
                                    <td>{e.username}</td>
                                    <td>{e.email}</td>
                                    <td>{e.personal_token}</td>
                                    <td>{e.amount_of_processed_records}</td>
                                    <td>{e.processing_timestamp}</td>
                                    <td>{e.status}</td>
                                </tr>
                            ))}

                        </tbody>
                    </table>

                </div>
            </div>
        );
    }
}
export default ProcessingHistory;