import React, { useEffect, useState } from 'react';
import user from "./user.png"
import { useNavigate } from "react-router-dom";


const EditUser = () => {

    const loginUser = JSON.parse(localStorage.getItem('loggeduser'));
    const [error, setError] = useState(null);
    const navigate = useNavigate();

    const [formData, setFormData] = useState({
        firstname: "",
        lastname: "",
        username: "",
        email: "",
        phone: ""
    });




    useEffect(() => {
        if (loginUser) {
            const headers = new Headers();
            headers.set('Authorization', `Basic ${btoa(`${loginUser.username}:${loginUser.password}`)}`);
            headers.set('content-type', 'application/json');

            fetch(`http://127.0.0.1:5000/api/v1/user/${loginUser.username}`, {
                method: 'GET',
                headers,
            }).then((response) => {
                if (response.status === 200) {
                    return response.json();
                }
            }).then((data) => {
                setFormData({
                    firstname: data.user.firstname, lastname: data.user.lastname, username: data.user.username,
                    email: data.user.email, phone: data.user.phone
                })
            })
        } else {
            navigate('/login');
        }
    }, []);

    const handleChange = e => {
        setError(null);
        setFormData({ ...formData, [e.target.name]: e.target.value });
    };


    const handleSubmit = async e => {
        e.preventDefault();

        const headers = new Headers();
        headers.set('Authorization', `Basic ${btoa(`${loginUser.username}:${loginUser.password}`)}`);
        headers.set('content-type', 'application/json');

        setError(null);

        const data = {
            firstname: formData.firstname,
            lastname: formData.lastname,
            username: formData.username,
            email: formData.email,
            phone: formData.phone
        };

        fetch(`http://127.0.0.1:5000/api/v1/user/info/${loginUser.username}`, {
            method: 'PUT',
            body: JSON.stringify(data),
            headers,
        }).then((response) => {
            if (response.status === 200) {
                loginUser.username = data.username;
                localStorage.setItem('loggeduser', JSON.stringify(loginUser));
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

    const handleDelete = async e => {
        e.preventDefault();

        const headers = new Headers();

        headers.set('Authorization', 'Basic ' + btoa(loginUser.username + ":" + loginUser.password));
        headers.set('content-type', 'application/json');


        fetch(`http://127.0.0.1:5000/api/v1/user/${loginUser.username}`, {
            method: 'DELETE',
            headers,
        }).then((response) => {
            if (response.status === 200) {
                localStorage.removeItem('loggeduser');
                navigate("/login")
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
                <div class="col-md-4">

                    <div class="card shadow mb-5 p-5">
                        <h2 class="text-center text-uppercase">Edit account</h2>
                        <hr />

                        <img class="user-photo" src={user} />

                        <div>
                            <button class="btn btn_login btn-primary btn-block rounded-pill" onClick={handleSubmit} type="submit" >Save</button>
                            <button class="btn btn_login btn-danger btn-block rounded-pill" onClick={handleDelete} type="submit" >Delete</button>
                        </div>
                    </div>
                </div>
                <div class="col-md-8">
                    <div class="card shadow mb-5 p-5">
                        <h2 class="text-center text-uppercase">Your information:</h2>
                        <hr />
                        <div class="form-group">
                            <label>Name</label>
                            <input placeholder="Name" type="text" name="firstname" value={formData.firstname} onChange={handleChange} />
                        </div>
                        <div class="form-group">
                            <label>Surname</label>
                            <input placeholder="Surname" type="text" name="lastname" value={formData.lastname} onChange={handleChange} />
                        </div>
                        <div class="form-group">
                            <label>Login</label>
                            <input placeholder="Login" type="text" name="username" value={formData.username} onChange={handleChange} />
                        </div>
                        <div class="form-group">
                            <label>Email</label>
                            <input placeholder="Email" type="email" name="email" value={formData.email} onChange={handleChange} />
                        </div>
                        <div class="form-group">
                            <label>Phone</label>
                            <input placeholder="Phone" type="text" name="phone" value={formData.phone} onChange={handleChange} />
                        </div>
                    </div>

                </div>
            </div>
            <p className="error">{error}</p>

        </div>

    );
}

export default EditUser;