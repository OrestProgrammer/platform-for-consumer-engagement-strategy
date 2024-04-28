import React, { useState } from 'react';
import "./styles.css"
import { Link, useNavigate } from "react-router-dom";
import axios from "axios";


const Register = () => {

    const navigate = useNavigate();

    const [formData, setFormData] = useState({
        firstname: "",
        lastname: "",
        username: "",
        password: "",
        email: "",
        phone: ""
    });

    const [error, setError] = useState(null);

    const handleChange = e => {
        setError(null);
        setFormData({ ...formData, [e.target.name]: e.target.value });
    };

    const handleSubmit = async e => {
        e.preventDefault();

        setError(null);
        const data = {
            firstname: formData.firstname,
            lastname: formData.lastname,
            username: formData.username,
            password: formData.password,
            email: formData.email,
            phone: formData.phone
        };

        fetch('http://127.0.0.1:5000/api/v1/user', {
            method: 'POST',
            body: JSON.stringify(data),
            headers: { 'Content-Type': 'application/json' },
        }).then((response) => {
            if (response.status === 200) {
                window.localStorage.setItem('loggeduser', JSON.stringify(data));
                navigate('/mainuserpage')
            } else {
                response.text().then((data) => {
                    setError(data)
                });
            }
        }).catch((e) => {
            console.log(e)
        });
    };

    return (
        <div class="container mt-5">
            <div class="row">
                <div class="col"></div>
                <div class="col-md-5 ">

                    <div class="left card shadow mb-5 p-5">
                        <h2 class="text-center text-uppercase">Sign Up</h2>
                        <hr />
                        <form data-testid="registerform" onSubmit={handleSubmit}>

                            <div class="form-group">
                                <label>First Name</label>
                                <input placeholder="Your First Name" type="text" name="firstname" required minLength="3" value={formData.firstname}
                                    onChange={handleChange} />
                            </div>

                            <div class="form-group">
                                <label>Your Last Name</label>
                                <input placeholder="Last Name" type="text" name="lastname" required minLength="3" value={formData.lastname}
                                    onChange={handleChange} />
                            </div>
                            <div class="form-group">
                                <label>Your Login</label>
                                <input placeholder="Login" type="text" name="username" required minLength="3" value={formData.username}
                                    onChange={handleChange} />
                            </div>
                            <div class="form-group">
                                <label>Your Password</label>
                                <input placeholder="Password" type="password" name="password" required
                                    minLength="6" value={formData.password} onChange={handleChange} />
                            </div>
                            <div class="form-group">
                                <label>Your Email</label>
                                <input placeholder="Email" type="email" name="email" required minLength="3" value={formData.email} onChange={handleChange} />
                            </div>
                            <div class="form-group">
                                <label>Your Phone</label>
                                <input placeholder="Phone" type="text" name="phone" required minLength="3" value={formData.phone} onChange={handleChange} />
                            </div>
                            <button type="submit" class="btn btn_login btn-primary btn-block" name="login">Register</button>


                            <h6 class="mt-3"> Do you have an account? <Link to={"/login"}>Sign In</Link></h6>

                        </form>
                        <p className='error-message'>{error}</p>
                    </div>
                </div>
                <div class="col"></div>
            </div>
        </div>
    );
}

export default Register;