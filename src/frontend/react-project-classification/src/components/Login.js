import React, { useState } from 'react';
import "./styles.css"
import { Link, useNavigate, Navigate } from "react-router-dom";


const Login = () => {
    const currentUser = localStorage.getItem('loggeduser');

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

    if (currentUser) {
        return <Navigate to={'/mainuserpage'}>  </Navigate>
    }

    const handleChange = e => {
        setError(null);
        setFormData({ ...formData, [e.target.name]: e.target.value });
    };

    const handleSubmit = async e => {
        e.preventDefault();

        setError(null);
        const data = {
            username: formData.username,
            password: formData.password
        };

        fetch('http://127.0.0.1:5000/api/v1/user/login', {
            method: 'POST',
            body: JSON.stringify(data),
            headers: { 'Content-Type': 'application/json' },
        }).then((response) => {
            if (response.status === 200) {
                window.localStorage.setItem('loggeduser', JSON.stringify(data));
                navigate("/home")
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

                    <div class="card shadow mb-5 p-5">
                        <h2 class="text-center text-uppercase">LOGIN</h2>
                        <hr />

                        <form onSubmit={handleSubmit}>

                            <div class="form-group">
                                <label>Username</label>
                                <input placeholder="Login" type="text" name="username" required minLength="3" value={formData.username}
                                    onChange={handleChange} />
                            </div>


                            <div class="form-group">
                                <label>Password</label>
                                <input placeholder="Password" type="password" name="password" required
                                    minLength="6" value={formData.password} onChange={handleChange} />

                            </div>

                            <button type="submit" class="btn btn_login btn-primary btn-block" name="login">Sign In</button>

                            <h6 class="mt-3"> Don't Have account ? <Link to={"/register"}>Create Account Here</Link> </h6>


                        </form>

                        <p className='error-message'>{error}</p>

                    </div>
                </div>
                <div class="col"></div>
            </div>
        </div>
    );
}

export default Login;