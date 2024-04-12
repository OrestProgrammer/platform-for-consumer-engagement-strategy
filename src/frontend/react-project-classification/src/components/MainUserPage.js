import React, { useEffect, useState } from 'react';
import photo from "./user.png"
import { Link, useNavigate } from "react-router-dom";


const MainUserPage = () => {

    const loginUser = JSON.parse(localStorage.getItem('loggeduser'));
    const [user, setUser] = useState(loginUser);
    const navigate = useNavigate();

    useEffect(() => {
        if (loginUser) {
            const headers = new Headers();

            headers.set('Authorization', 'Basic ' + btoa(loginUser.username + ":" + loginUser.password));
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


    const handleLogOut = async e => {
        e.preventDefault();


        localStorage.removeItem('loggeduser');
        navigate("/home")
    };

    const handleEditUser = async e => {
        e.preventDefault();
        navigate("/edituser")
    };


    if (!loginUser) {
        navigate("/login")
    } else {
        return (
            <div class="container mt-5">
                <div class="row">
                    <div class="col-md-4">

                        <div class="card shadow mb-5 p-5">
                            <h2 class="text-center text-uppercase">Account</h2>
                            <hr />
                            <img class="user-photo" src={photo} />

                            <h2 class="mt-2 text-center" >{user && user.firstname} {user && user.lastname}</h2>
                            <p class="text-center">{user && user.email}</p>
                            <div>
                                <button class="btn btn_login btn-danger btn-block" onClick={handleLogOut} type="submit" >Log out</button>

                                <button onClick={handleEditUser} class="btn btn_login btn-primary btn-block" type="submit">Edit account</button>

                            </div>

                        </div>

                    </div>
                    <div class="col-md-8">
                        <div class="card shadow mb-5 p-5">
                            <h2 class="text-center text-uppercase">Your information:</h2>
                            <hr />
                            <ul class="list-group">
                                <li class="list-group-item"><b> First Name:</b> {user && user.firstname}</li>
                                <li class="list-group-item"><b>Last Name:</b>  {user && user.lastname}</li>
                                <li class="list-group-item"><b>Login</b> {user && user.username}</li>
                                <li class="list-group-item"><b>Email:</b>  {user && user.email}</li>
                                <li class="list-group-item"><b> Phone:</b> {user && user.phone}</li>
                                <li class="list-group-item"><b>Personal Token:</b>  {user && user.personal_token}</li>
                                <li class="list-group-item"><b>Amount of records to process left:</b>  {user && user.processing_rows_left}</li>
                            </ul>
                        </div>

                    </div>
                </div>
            </div>
        );
    }
}
export default MainUserPage;