import React from 'react';
import { Link, useNavigate, useLocation } from "react-router-dom";

const Navbar = () => {
    const navigate = useNavigate();

    const loginUser = JSON.parse(localStorage.getItem('loggeduser'));

    const handleLogout = () => {
        localStorage.removeItem('loggeduser');
        navigate('/home');
    };

    const handleRedirectUser = () => {
        navigate('/mainuserpage');
    };


    const location = useLocation();

    function handleActive(path) {

        return location.pathname === path ? 'active' : '';
    }

    return (
        <nav className="navbar navbar-expand-lg">
            <div className="container-fluid justify-content-between">
                <div>
                    <Link to={"/home"} className={`navbar-brand ${handleActive('/home')}`}>
                        Consumer Classification Platform
                    </Link>
                </div>
                <ul className="navbar-nav">
                    <li className="nav-item search">
                        <input placeholder="Search" type="text" name="xz" />
                    </li>
                    {loginUser ? (
                        <>
                            <li className="nav-item">
                                <button onClick={handleRedirectUser} className={`nav-link ${handleActive('/mainuserpage')}`}>User</button>
                            </li>
                        </>
                    ) : (
                        <li className="nav-item">
                            <Link to="/login" className="nav-link">Login</Link>
                        </li>
                    )}
                </ul>
            </div>
        </nav>
    );
}

export default Navbar;