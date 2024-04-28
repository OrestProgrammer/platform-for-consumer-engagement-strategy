import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';

const Payment = () => {

    const [useVoucher, setUseVoucher] = useState(false);
    const [voucherCode, setVoucherCode] = useState("");
    const [amount, setAmount] = useState("");
    const [totalPrice, setTotalPrice] = useState(0);
    const [pricePerItem] = useState(0.001);
    const navigate = useNavigate();
    const [errorMsg, setErrorMsg] = useState(null);

    const loginUser = JSON.parse(localStorage.getItem('loggeduser'));
    const [user, setUser] = useState(loginUser);

    const handleVoucherChange = e => {
        setVoucherCode(e.target.value);
    };

    const handleAmountChange = e => {
        setAmount(e.target.value);
        setTotalPrice((e.target.value * pricePerItem).toFixed(2));
    };

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

    const handleVoucherSubmit = e => {
        e.preventDefault();

        const headers = new Headers();
        headers.set('Authorization', `Basic ${btoa(`${loginUser.username}:${loginUser.password}`)}`);
        headers.set('content-type', 'application/json');

        const username = loginUser.username

        setErrorMsg(null); // reset error message before new request

        fetch('http://127.0.0.1:5000/api/v1/voucher', {
            method: 'POST',
            body: JSON.stringify({ voucherCode, username }),
            headers,
        })
            .then(response => response.json())
            .then(data => {
                if (data.status === 'error') {
                    setErrorMsg(data.message);
                } else {
                    navigate("/mainuserpage")
                }
            })
            .catch((error) => {
                console.error('Error:', error);
                setErrorMsg("Something went wrong. Please try again.");
            });
    };

    const handlePurchase = e => {
        e.preventDefault();

        const headers = new Headers();
        headers.set('Authorization', `Basic ${btoa(`${loginUser.username}:${loginUser.password}`)}`);
        headers.set('content-type', 'application/json');

        const username = loginUser.username

        setErrorMsg(null); // reset error message before new request

        fetch('http://127.0.0.1:5000/api/v1/payment', {
            method: 'POST',
            body: JSON.stringify({ amount, totalPrice, username }),
            headers,
        })
            .then(response => response.json())
            .then(data => {
                if (data.status === 'error') {
                    setErrorMsg(data.message);
                } else {
                    navigate("/mainuserpage")
                }
            })
            .catch((error) => {
                console.error('Error:', error);
                setErrorMsg("Something went wrong. Please try again.");
            });
    };

    return (
        <div class="container text-center mt-5">
            <div class="card shadow mb-5 p-5">
                <form>
                    <h2 class="mb-3">Payment Screen</h2>
                    <div class="button-group">

                        <button class={!useVoucher ? 'btn btn-success' : 'btn btn-light'} onClick={() => setUseVoucher(!useVoucher)} type="button">Cash</button>
                        <button class={useVoucher ? 'btn btn-success' : 'btn btn-light'} onClick={() => setUseVoucher(!useVoucher)} type="button">Voucher</button>
                    </div>

                    {useVoucher ? (
                        <div class="payment">
                            <div class="form-group">
                                <label>Use Voucher</label>
                                <input placeholder="Enter Voucher" type="text"
                                    value={voucherCode}
                                    onChange={handleVoucherChange} />
                            </div>
                            <button onClick={handleVoucherSubmit} type="button" class="btn btn-primary">Use Voucher</button>

                        </div>

                    ) : (
                        <div class="payment">
                            <div class="form-group">
                                <label>Use quantity</label>
                                <input type="number"
                                    min="1"
                                    placeholder="Enter quantity"
                                    value={amount}
                                    onChange={handleAmountChange} />


                            </div>
                            <h2 class="mt-3">Total Price: {totalPrice}</h2>

                            <button onClick={handlePurchase} type="button" class="btn btn-primary">Pay Cash</button>

                        </div>
                    )}
                    {errorMsg && <p className="error-message">{errorMsg}</p>}
                </form>
            </div>

        </div>
    );
}

export default Payment;